package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"syslog-server/types"
)

type Reader interface {
	GetRecentMessages(limit int) ([]*types.SyslogMessage, error)
	GetMessagesByDate(date time.Time) ([]*types.SyslogMessage, error)
	GetMessagesPaginated(offset, limit int) ([]*types.SyslogMessage, int, error)
	GetTotalMessageCount() (int, error)
}

type Storage interface {
	Store(message *types.SyslogMessage) error
}

type FileStorage struct {
	baseDir    string
	writeMutex sync.Mutex
	readMutex  sync.RWMutex

	// 🚀 CACHE IN-MEMORY
	recentCache    []*types.SyslogMessage
	maxRecentCache int
	totalCountCache int
	countCachedAt  time.Time
	countCacheTTL  time.Duration
	cacheMutex     sync.RWMutex
}

func NewFileStorage(baseDir string) (*FileStorage, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, err
	}

	fs := &FileStorage{
		baseDir:        baseDir,
		maxRecentCache: 500,              // Păstrăm ultimele 500 mesaje direct în memorie
		countCacheTTL:  30 * time.Second, // Recalculăm de pe disc la fiecare 30 sec doar dacă e nevoie
	}

	// Warmup cache la pornire
	go fs.warmupCache()

	return fs, nil
}

// warmupCache populează cache-ul la pornirea aplicației
func (fs *FileStorage) warmupCache() {
	fs.cacheMutex.Lock()
	defer fs.cacheMutex.Unlock()

	// Populează ultimele mesaje
	messages, err := fs.loadRecentFromDisk(fs.maxRecentCache)
	if err == nil {
		fs.recentCache = messages
	}

	// Calculează count-ul inițial
	count, err := fs.countFromDisk()
	if err == nil {
		fs.totalCountCache = count
		fs.countCachedAt = time.Now()
	}
}

func (fs *FileStorage) Store(message *types.SyslogMessage) error {
	fs.writeMutex.Lock()

	// 1. Scriere pe disc (Log TXT)
	filename := fmt.Sprintf("syslog-%s.log", message.Timestamp.Format("2006-01-02"))
	filePath := filepath.Join(fs.baseDir, filename)

	timestamp := message.Timestamp.Format("2006-01-02 15:04:05")
	logEntry := fmt.Sprintf("[%s] %s %s[%s]: %s\n",
		timestamp,
		message.Hostname,
		message.Tag,
		message.SeverityString(),
		message.Content,
	)

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fs.writeMutex.Unlock()
		return err
	}

	if _, err := file.WriteString(logEntry); err != nil {
		file.Close()
		fs.writeMutex.Unlock()
		return err
	}
	file.Close()

	// 2. Scriere pe disc (JSON Entry)
	jsonFilename := fmt.Sprintf("syslog-%s.json", message.Timestamp.Format("2006-01-02"))
	jsonFilePath := filepath.Join(fs.baseDir, jsonFilename)

	jsonFile, err := os.OpenFile(jsonFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fs.writeMutex.Unlock()
		return err
	}

	jsonEntry := map[string]interface{}{
		"timestamp":   message.Timestamp,
		"hostname":    message.Hostname,
		"tag":         message.Tag,
		"content":     message.Content,
		"severity":    message.Severity,
		"facility":    message.Facility,
		"remote_ip":   message.RemoteIP,
		"raw_message": message.RawMessage,
	}

	encoder := json.NewEncoder(jsonFile)
	err = encoder.Encode(jsonEntry)
	jsonFile.Close()
	fs.writeMutex.Unlock()

	if err != nil {
		return err
	}

	// 3. 🚀 UPDATE IN-MEMORY CACHE (Instantaneu, fără Disk I/O)
	fs.cacheMutex.Lock()
	// Prepend mesajul nou la începutul fâșiei
	fs.recentCache = append([]*types.SyslogMessage{message}, fs.recentCache...)
	if len(fs.recentCache) > fs.maxRecentCache {
		fs.recentCache = fs.recentCache[:fs.maxRecentCache]
	}
	// Incrementează contorul total
	fs.totalCountCache++
	fs.cacheMutex.Unlock()

	return nil
}

func (fs *FileStorage) Close() error {
	return nil
}

// GetRecentMessages returnează ultimele N mesaje din CACHE (Dacă e posibil)
func (fs *FileStorage) GetRecentMessages(limit int) ([]*types.SyslogMessage, error) {
	fs.cacheMutex.RLock()
	if len(fs.recentCache) >= limit {
		// Slice copy pentru a preveni race conditions
		result := make([]*types.SyslogMessage, limit)
		copy(result, fs.recentCache[:limit])
		fs.cacheMutex.RUnlock()
		return result, nil
	}
	fs.cacheMutex.RUnlock()

	// Fallback la citire de pe disc doar dacă cache-ul are mai puține mesaje decât cererea
	return fs.loadRecentFromDisk(limit)
}

// GetTotalMessageCount returnează numărul total de mesaje din Cache
func (fs *FileStorage) GetTotalMessageCount() (int, error) {
	fs.cacheMutex.RLock()
	if time.Since(fs.countCachedAt) < fs.countCacheTTL && fs.totalCountCache > 0 {
		count := fs.totalCountCache
		fs.cacheMutex.RUnlock()
		return count, nil
	}
	fs.cacheMutex.RUnlock()

	// Recalculează de pe disc dacă e primul apel / expiră TTL
	count, err := fs.countFromDisk()
	if err != nil {
		return 0, err
	}

	fs.cacheMutex.Lock()
	fs.totalCountCache = count
	fs.countCachedAt = time.Now()
	fs.cacheMutex.Unlock()

	return count, nil
}

// GetMessagesPaginated folosește cache-ul pentru prima pagină sau citește optimizat
func (fs *FileStorage) GetMessagesPaginated(offset, limit int) ([]*types.SyslogMessage, int, error) {
	total, err := fs.GetTotalMessageCount()
	if err != nil {
		return nil, 0, err
	}

	// 🚀 Scurtcircuitare: Dacă cere prima pagină și intra în recentCache
	fs.cacheMutex.RLock()
	if offset == 0 && limit <= len(fs.recentCache) {
		result := make([]*types.SyslogMessage, limit)
		copy(result, fs.recentCache[:limit])
		fs.cacheMutex.RUnlock()
		return result, total, nil
	}
	fs.cacheMutex.RUnlock()

	// Fallback de pe disc pentru pagini vechi
	fs.readMutex.RLock()
	defer fs.readMutex.RUnlock()

	var allMessages []*types.SyslogMessage
	files, err := filepath.Glob(filepath.Join(fs.baseDir, "syslog-*.json"))
	if err != nil {
		return nil, 0, err
	}

	sort.Sort(sort.Reverse(sort.StringSlice(files)))

	// Colectăm doar până la offset + limit pentru a nu încărca TOATE fișierele vechi
	needed := offset + limit
	for _, file := range files {
		messages, err := fs.readJSONFileUnsafe(file)
		if err != nil {
			continue
		}
		allMessages = append(allMessages, messages...)

		if len(allMessages) >= needed*2 {
			break
		}
	}

	sort.Slice(allMessages, func(i, j int) bool {
		return allMessages[i].Timestamp.After(allMessages[j].Timestamp)
	})

	start := offset
	end := offset + limit
	if start > len(allMessages) {
		return []*types.SyslogMessage{}, total, nil
	}
	if end > len(allMessages) {
		end = len(allMessages)
	}

	return allMessages[start:end], total, nil
}

func (fs *FileStorage) GetMessagesByDate(date time.Time) ([]*types.SyslogMessage, error) {
	fs.readMutex.RLock()
	defer fs.readMutex.RUnlock()

	filename := fmt.Sprintf("syslog-%s.json", date.Format("2006-01-02"))
	filePath := filepath.Join(fs.baseDir, filename)

	return fs.readJSONFileUnsafe(filePath)
}

// Helper: Încarcă mesaje recente de pe disc (pentru fallback sau warmup)
func (fs *FileStorage) loadRecentFromDisk(limit int) ([]*types.SyslogMessage, error) {
	fs.readMutex.RLock()
	defer fs.readMutex.RUnlock()

	var allMessages []*types.SyslogMessage
	files, err := filepath.Glob(filepath.Join(fs.baseDir, "syslog-*.json"))
	if err != nil {
		return nil, err
	}

	sort.Sort(sort.Reverse(sort.StringSlice(files)))

	for _, file := range files {
		messages, err := fs.readJSONFileUnsafe(file)
		if err != nil {
			continue
		}
		allMessages = append(allMessages, messages...)

		if len(allMessages) >= limit*2 {
			break
		}
	}

	sort.Slice(allMessages, func(i, j int) bool {
		return allMessages[i].Timestamp.After(allMessages[j].Timestamp)
	})

	if len(allMessages) > limit {
		allMessages = allMessages[:limit]
	}

	return allMessages, nil
}

// Helper: Numără mesajele de pe disc
func (fs *FileStorage) countFromDisk() (int, error) {
	fs.readMutex.RLock()
	defer fs.readMutex.RUnlock()

	var count int
	files, err := filepath.Glob(filepath.Join(fs.baseDir, "syslog-*.json"))
	if err != nil {
		return 0, err
	}

	for _, file := range files {
		messages, err := fs.readJSONFileUnsafe(file)
		if err != nil {
			continue
		}
		count += len(messages)
	}

	return count, nil
}

func (fs *FileStorage) readJSONFileUnsafe(filename string) ([]*types.SyslogMessage, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var messages []*types.SyslogMessage
	decoder := json.NewDecoder(file)

	for decoder.More() {
		var msgData map[string]interface{}
		if err := decoder.Decode(&msgData); err != nil {
			continue
		}

		msg, err := fs.parseMessageFromJSON(msgData)
		if err != nil {
			continue
		}
		messages = append(messages, msg)
	}

	return messages, nil
}

func (fs *FileStorage) parseMessageFromJSON(data map[string]interface{}) (*types.SyslogMessage, error) {
	msg := &types.SyslogMessage{}

	if timestamp, exists := data["timestamp"]; exists {
		switch t := timestamp.(type) {
		case string:
			formats := []string{
				time.RFC3339,
				"2006-01-02 15:04:05",
				"2006-01-02T15:04:05Z",
				"2006-01-02 15:04:05 -0700",
				time.RFC3339Nano,
			}

			parsed := false
			for _, format := range formats {
				if parsedTime, err := time.Parse(format, t); err == nil {
					msg.Timestamp = parsedTime
					parsed = true
					break
				}
			}

			if !parsed {
				msg.Timestamp = time.Now()
			}
		default:
			msg.Timestamp = time.Now()
		}
	} else {
		msg.Timestamp = time.Now()
	}

	msg.Hostname = getString(data, "hostname", "unknown")
	msg.Tag = getString(data, "tag", "")
	msg.Content = getString(data, "content", "No content")
	msg.RemoteIP = getString(data, "remote_ip", "N/A")
	msg.RawMessage = getString(data, "raw_message", msg.Content)

	msg.Severity = getInt(data, "severity", 6)
	msg.Facility = getInt(data, "facility", 1)
	msg.Priority = (msg.Facility * 8) + msg.Severity

	return msg, nil
}

func getString(data map[string]interface{}, key string, defaultValue string) string {
	if value, ok := data[key].(string); ok && value != "" {
		return value
	}
	return defaultValue
}

func getInt(data map[string]interface{}, key string, defaultValue int) int {
	if value, ok := data[key].(float64); ok {
		return int(value)
	}
	return defaultValue
}