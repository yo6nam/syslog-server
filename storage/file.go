package storage

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
	recentCache     []*types.SyslogMessage
	maxRecentCache  int
	totalCountCache int
	countCachedAt   time.Time
	countCacheTTL   time.Duration
	cacheMutex      sync.RWMutex
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

	// Warmup sincron: la returnare cache-ul e consistent și niciun Store()
	// nu poate rula în paralel cu el (altfel apăreau mesaje duplicate).
	fs.warmupCache()

	return fs, nil
}

// warmupCache populează cache-ul la pornirea aplicației.
// Ține writeMutex ca să nu se intercaleze cu Store() (bug-ul de duplicare
// din commit-ul 0eef9d4). Numărarea completă NU se face aici: pe un director
// cu luni de loguri ar bloca startup-ul. Lăsăm countCachedAt zero, iar primul
// GetTotalMessageCount va recalcula lazy, la cerere.
func (fs *FileStorage) warmupCache() {
	fs.writeMutex.Lock()
	defer fs.writeMutex.Unlock()

	fs.cacheMutex.Lock()
	defer fs.cacheMutex.Unlock()

	messages, err := fs.loadRecentFromDisk(fs.maxRecentCache)
	if err == nil {
		fs.recentCache = messages
	}

	fs.countCachedAt = time.Time{}
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
	if !fs.countCachedAt.IsZero() && time.Since(fs.countCachedAt) < fs.countCacheTTL {
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

// SearchMessages caută în mesaje după termen, hostname, severitate și
// interval de date — echivalentul FileStorage al MySQLStorage.SearchMessages.
//
// Dacă atât dateFrom cât și dateTo sunt precizate, citim DOAR fișierele-zi
// (syslog-YYYY-MM-DD.json) care se suprapun cu intervalul cerut — nu tot
// corpusul. Pentru "ultimele 24h" asta înseamnă cel mult 2 fișiere (ziua
// curentă + ziua anterioară dacă intervalul trece de miezul nopții), nu un
// scan complet al retenției. Fără interval de date, căutăm în tot ce există
// pe disc (căutare generală, declanșată explicit de utilizator, nu pe fluxul
// live de mesaje). Notă: SearchMessages NU folosește recentCache — caută
// mereu direct pe disc, pentru corectitudine indiferent de interval.
func (fs *FileStorage) SearchMessages(searchTerm string, hostname string, severity int, severityMax int, dateFrom time.Time, dateTo time.Time, limit int, offset int) ([]*types.SyslogMessage, int, error) {
	fs.readMutex.RLock()
	defer fs.readMutex.RUnlock()

	var candidateFiles []string

	if !dateFrom.IsZero() && !dateTo.IsZero() {
		start := time.Date(dateFrom.Year(), dateFrom.Month(), dateFrom.Day(), 0, 0, 0, 0, dateFrom.Location())
		for d := start; !d.After(dateTo); d = d.AddDate(0, 0, 1) {
			filename := fmt.Sprintf("syslog-%s.json", d.Format("2006-01-02"))
			candidateFiles = append(candidateFiles, filepath.Join(fs.baseDir, filename))
		}
	} else {
		files, err := filepath.Glob(filepath.Join(fs.baseDir, "syslog-*.json"))
		if err != nil {
			return nil, 0, err
		}
		candidateFiles = files
	}

	sort.Sort(sort.Reverse(sort.StringSlice(candidateFiles)))

	searchTermLower := strings.ToLower(searchTerm)
	hostnameLower := strings.ToLower(hostname)

	var matched []*types.SyslogMessage
	for _, file := range candidateFiles {
		messages, err := fs.readJSONFileUnsafe(file)
		if err != nil {
			// Fișier lipsă (nicio zi cu mesaje în acea dată) sau corupt — sărim peste.
			continue
		}

		for _, msg := range messages {
			if !dateFrom.IsZero() && msg.Timestamp.Before(dateFrom) {
				continue
			}
			if !dateTo.IsZero() && msg.Timestamp.After(dateTo) {
				continue
			}
			if searchTerm != "" {
				contentMatch := strings.Contains(strings.ToLower(msg.Content), searchTermLower)
				rawMatch := strings.Contains(strings.ToLower(msg.RawMessage), searchTermLower)
				if !contentMatch && !rawMatch {
					continue
				}
			}
			if hostname != "" && !strings.Contains(strings.ToLower(msg.Hostname), hostnameLower) {
				continue
			}
			if severity >= 0 && severityMax >= 0 {
				if msg.Severity < severity || msg.Severity > severityMax {
					continue
				}
			} else if severity >= 0 && severity <= 7 {
				if msg.Severity != severity {
					continue
				}
			}

			matched = append(matched, msg)
		}
	}

	sort.Slice(matched, func(i, j int) bool {
		return matched[i].Timestamp.After(matched[j].Timestamp)
	})

	total := len(matched)

	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 5000 {
		limit = 5000
	}

	end := offset + limit
	if offset > total {
		return []*types.SyslogMessage{}, total, nil
	}
	if end > total {
		end = total
	}

	return matched[offset:end], total, nil
}

// DeleteOldMessages implementează retenția pentru file storage: șterge
// fișierele-zi întregi (syslog-YYYY-MM-DD.json și .log) mai vechi decât
// cutoffDate — nu rânduri individuale (nu are sens la fișiere JSON append-only).
// Fără asta, RetentionManager nu avea NICIUN efect pe file storage: mesajele
// rămâneau pe disc la nesfârșit, indiferent de retention.days din config.
func (fs *FileStorage) DeleteOldMessages(cutoffDate time.Time) (int, error) {
	fs.writeMutex.Lock() // evită coliziune cu Store(), care scrie concurent fișierul zilei curente
	defer fs.writeMutex.Unlock()

	files, err := filepath.Glob(filepath.Join(fs.baseDir, "syslog-*.json"))
	if err != nil {
		return 0, err
	}

	cutoffDay := cutoffDate.Format("2006-01-02")
	deletedMessages := 0

	for _, jsonFile := range files {
		base := filepath.Base(jsonFile)
		dayStr := strings.TrimSuffix(strings.TrimPrefix(base, "syslog-"), ".json")
		if len(dayStr) != len("2006-01-02") {
			continue // nume de fișier neașteptat — sărim peste, nu riscăm să ștergem greșit
		}
		if dayStr >= cutoffDay {
			continue // ziua asta e încă în interiorul perioadei de retenție
		}

		// Numărăm mesajele din fișier înainte să-l ștergem, doar pentru raportarea din log.
		if messages, err := fs.readJSONFileUnsafe(jsonFile); err == nil {
			deletedMessages += len(messages)
		}

		if err := os.Remove(jsonFile); err != nil && !os.IsNotExist(err) {
			log.Printf("⚠️  Retention: failed to remove %s: %v", jsonFile, err)
			continue
		}

		logFile := filepath.Join(fs.baseDir, fmt.Sprintf("syslog-%s.log", dayStr))
		if err := os.Remove(logFile); err != nil && !os.IsNotExist(err) {
			log.Printf("⚠️  Retention: failed to remove %s: %v", logFile, err)
		}
	}

	// Invalidăm cache-ul de count (nu recentCache — mesajele șterse sunt oricum
	// mult mai vechi decât ce ține recentCache) ca următorul apel să recalculeze
	// corect numărul total de pe disc, reflectând fișierele șterse.
	fs.cacheMutex.Lock()
	fs.countCachedAt = time.Time{}
	fs.cacheMutex.Unlock()

	return deletedMessages, nil
}

// loadRecentFromDisk încarcă mesajele recente de pe disc pentru warmup/fallback.
// Pentru a nu bloca startup-ul pe un director cu luni de loguri, citim doar
// fișierele-zi din ultimele 72h (sau, dacă nu există, ultimele fișiere
// disponibile) și ne oprim după warmupMaxMessages mesaje.
func (fs *FileStorage) loadRecentFromDisk(limit int) ([]*types.SyslogMessage, error) {
	const (
		warmupMaxMessages = 5000
		warmupMaxAge      = 72 * time.Hour
	)

	fs.readMutex.RLock()
	defer fs.readMutex.RUnlock()

	files, err := filepath.Glob(filepath.Join(fs.baseDir, "syslog-*.json"))
	if err != nil {
		return nil, err
	}
	if len(files) == 0 {
		return nil, nil
	}

	// Păstrăm doar fișierele-zi din ultimele 72h. Numele e syslog-YYYY-MM-DD.json,
	// deci comparăm lexicografic ziua — suficient și fără parse de dată.
	cutoff := time.Now().Add(-warmupMaxAge).Format("2006-01-02")
	var recentFiles []string
	for _, f := range files {
		base := filepath.Base(f)
		dayStr := strings.TrimSuffix(strings.TrimPrefix(base, "syslog-"), ".json")
		if len(dayStr) != len("2006-01-02") {
			continue // nume neașteptat — ignorăm
		}
		if dayStr >= cutoff {
			recentFiles = append(recentFiles, f)
		}
	}

	// Dacă nu există nimic în ultimele 72h (ex. server oprit mai mult),
	// cădem pe ultimele fișiere disponibile, ca să nu pornim cu cache gol.
	if len(recentFiles) == 0 {
		sort.Sort(sort.Reverse(sort.StringSlice(files)))
		maxFiles := 3
		if len(files) < maxFiles {
			maxFiles = len(files)
		}
		recentFiles = files[:maxFiles]
	}

	// Cele mai noi primele, ca să umplem recentCache cu ce trebuie.
	sort.Sort(sort.Reverse(sort.StringSlice(recentFiles)))

	// Plafonul efectiv: nu mai mult de warmupMaxMessages, dar nici mai mult
	// decât cere apelantul (limit) — warmup vrea maxRecentCache, fallback vrea N.
	capMessages := limit * 2
	if capMessages == 0 || capMessages > warmupMaxMessages {
		capMessages = warmupMaxMessages
	}

	var allMessages []*types.SyslogMessage
	for _, file := range recentFiles {
		messages, err := fs.readJSONFileUnsafe(file)
		if err != nil {
			continue
		}
		allMessages = append(allMessages, messages...)

		if len(allMessages) >= capMessages {
			break
		}
	}

	sort.Slice(allMessages, func(i, j int) bool {
		return allMessages[i].Timestamp.After(allMessages[j].Timestamp)
	})

	if len(allMessages) > limit && limit > 0 {
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
