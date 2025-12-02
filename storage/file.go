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
	writeMutex sync.Mutex // Pentru operații de scriere
	readMutex  sync.RWMutex // Pentru operații de citire
}

func NewFileStorage(baseDir string) (*FileStorage, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, err
	}

	return &FileStorage{
		baseDir: baseDir,
	}, nil
}

func (fs *FileStorage) Store(message *types.SyslogMessage) error {
	fs.writeMutex.Lock()
	defer fs.writeMutex.Unlock()

	// Create filename based on current date
	filename := fmt.Sprintf("syslog-%s.log", message.Timestamp.Format("2006-01-02"))
	filePath := filepath.Join(fs.baseDir, filename)

	// Format log entry
	timestamp := message.Timestamp.Format("2006-01-02 15:04:05")
	logEntry := fmt.Sprintf("[%s] %s %s[%s]: %s\n",
		timestamp,
		message.Hostname,
		message.Tag,
		message.SeverityString(),
		message.Content,
	)

	// Append to file
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := file.WriteString(logEntry); err != nil {
		return err
	}

	// Also store in JSON format for better querying
	jsonFilename := fmt.Sprintf("syslog-%s.json", message.Timestamp.Format("2006-01-02"))
	jsonFilePath := filepath.Join(fs.baseDir, jsonFilename)

	jsonFile, err := os.OpenFile(jsonFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer jsonFile.Close()

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
	if err := encoder.Encode(jsonEntry); err != nil {
		return err
	}

	return nil
}

func (fs *FileStorage) Close() error {
	return nil
}

// GetRecentMessages returnează ultimele N mesaje din logurile JSON
func (fs *FileStorage) GetRecentMessages(limit int) ([]*types.SyslogMessage, error) {
	fs.readMutex.RLock()
	defer fs.readMutex.RUnlock()

	var allMessages []*types.SyslogMessage

	// Citește toate fișierele JSON din directorul de loguri
	files, err := filepath.Glob(filepath.Join(fs.baseDir, "syslog-*.json"))
	if err != nil {
		return nil, err
	}

	// Sortează fișierele după dată (cele mai recente primele)
	sort.Sort(sort.Reverse(sort.StringSlice(files)))

	for _, file := range files {
		messages, err := fs.readJSONFileUnsafe(file)
		if err != nil {
			continue
		}
		allMessages = append(allMessages, messages...)

		// Oprire dacă am colectat suficiente mesaje
		if len(allMessages) >= limit*2 {
			break
		}
	}

	// Sortează mesajele după timestamp (cele mai recente primele)
	sort.Slice(allMessages, func(i, j int) bool {
		return allMessages[i].Timestamp.After(allMessages[j].Timestamp)
	})

	// Limitează numărul de mesaje returnate
	if len(allMessages) > limit {
		allMessages = allMessages[:limit]
	}

	return allMessages, nil
}

// GetMessagesPaginated returnează mesaje paginate
func (fs *FileStorage) GetMessagesPaginated(offset, limit int) ([]*types.SyslogMessage, int, error) {
	fs.readMutex.RLock()
	defer fs.readMutex.RUnlock()

	var allMessages []*types.SyslogMessage

	// Citește toate fișierele JSON
	files, err := filepath.Glob(filepath.Join(fs.baseDir, "syslog-*.json"))
	if err != nil {
		return nil, 0, err
	}

	// Sortează fișierele după dată (cele mai recente primele)
	sort.Sort(sort.Reverse(sort.StringSlice(files)))

	// Colectează toate mesajele
	for _, file := range files {
		messages, err := fs.readJSONFileUnsafe(file)
		if err != nil {
			continue
		}
		allMessages = append(allMessages, messages...)
	}

	// Sortează mesajele după timestamp (cele mai recente primele)
	sort.Slice(allMessages, func(i, j int) bool {
		return allMessages[i].Timestamp.After(allMessages[j].Timestamp)
	})

	total := len(allMessages)

	// Aplică paginarea
	start := offset
	end := offset + limit
	if end > total {
		end = total
	}
	if start > total {
		start = total
	}

	if start < 0 {
		start = 0
	}

	var paginatedMessages []*types.SyslogMessage
	if start < total {
		paginatedMessages = allMessages[start:end]
	} else {
		paginatedMessages = []*types.SyslogMessage{}
	}

	return paginatedMessages, total, nil
}

// GetTotalMessageCount returnează numărul total de mesaje
func (fs *FileStorage) GetTotalMessageCount() (int, error) {
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

// GetMessagesByDate returnează mesaje pentru o dată specificată
func (fs *FileStorage) GetMessagesByDate(date time.Time) ([]*types.SyslogMessage, error) {
	fs.readMutex.RLock()
	defer fs.readMutex.RUnlock()

	filename := fmt.Sprintf("syslog-%s.json", date.Format("2006-01-02"))
	filePath := filepath.Join(fs.baseDir, filename)

	return fs.readJSONFileUnsafe(filePath)
}

// readJSONFileUnsafe citește un fișier JSON și returnează mesajele
// UNSAFE: Trebuie apelat doar când readMutex/writeMutex este deja locked
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

// parseMessageFromJSON parsează datele JSON în SyslogMessage
func (fs *FileStorage) parseMessageFromJSON(data map[string]interface{}) (*types.SyslogMessage, error) {
	msg := &types.SyslogMessage{}

	// Parsează timestamp
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

	// Parsează câmpurile string
	msg.Hostname = getString(data, "hostname", "unknown")
	msg.Tag = getString(data, "tag", "")
	msg.Content = getString(data, "content", "No content")
	msg.RemoteIP = getString(data, "remote_ip", "N/A")
	msg.RawMessage = getString(data, "raw_message", msg.Content)

	// Parsează câmpurile numerice
	msg.Severity = getInt(data, "severity", 6)
	msg.Facility = getInt(data, "facility", 1)
	msg.Priority = (msg.Facility * 8) + msg.Severity

	return msg, nil
}

// Helper functions
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