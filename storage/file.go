package storage

import (
	"bufio"
	"bytes"
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

	// 🚀 CACHE IN-MEMORY al ultimelor mesaje (pentru prima pagină / live view)
	recentCache    []*types.SyslogMessage
	maxRecentCache int
	cacheMutex     sync.RWMutex

	// Index de numărare pe fișier; discul e sursa de adevăr (vezi file_index.go).
	counts  map[string]*fileCount
	countMu sync.Mutex
}

func NewFileStorage(baseDir string) (*FileStorage, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, err
	}

	fs := &FileStorage{
		baseDir:        baseDir,
		maxRecentCache: 500, // Păstrăm ultimele 500 mesaje direct în memorie
		counts:         make(map[string]*fileCount),
	}

	// Warmup SINCRON și rapid (citește doar coada fișierelor): înainte rula
	// asincron, ținea cacheMutex cât citea tot istoricul (blocând Store()) și
	// se bătea cu Store() pe contor și pe recentCache.
	fs.warmupRecent()

	return fs, nil
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

// GetTotalMessageCount returnează numărul total de mesaje, calculat incremental
// de pe disc (vezi totalFromIndex): exact, fără contor în memorie care să diverge.
func (fs *FileStorage) GetTotalMessageCount() (int, error) {
	return fs.totalFromIndex()
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

	// Nu e nimic de invalidat: indexul de numărare observă singur fișierele
	// dispărute, iar recentCache conține mesaje mult mai noi decât cele șterse.
	return deletedMessages, nil
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

// readJSONFileUnsafe citește un fișier JSON-lines (un mesaj pe linie). Fiecare
// linie e parsată independent; liniile corupte, neterminate sau pline de NUL
// sunt sărite.
//
// Varianta veche folosea json.Decoder cu `for decoder.More() { if err :=
// decoder.Decode(..); err != nil { continue } }`. După o eroare de sintaxă
// Decoder-ul NU avansează: Decode întoarce aceeași eroare la nesfârșit, iar
// More() rămâne true, deci o singură linie coruptă (de exemplu cea rămasă
// neterminată după un restart sau o cădere de curent) bloca citirea într-o
// buclă infinită, cu un nucleu la 100% CPU.
func (fs *FileStorage) readJSONFileUnsafe(filename string) ([]*types.SyslogMessage, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var messages []*types.SyslogMessage
	reader := bufio.NewReaderSize(file, 64*1024)

	for {
		line, readErr := reader.ReadBytes('\n') // crește după nevoie: fără limită de lungime a liniei

		if len(bytes.TrimSpace(line)) > 0 {
			var msgData map[string]interface{}
			if err := json.Unmarshal(line, &msgData); err == nil {
				if msg, err := fs.parseMessageFromJSON(msgData); err == nil {
					messages = append(messages, msg)
				}
			}
		}

		if readErr != nil { // io.EOF sau eroare de citire: terminăm oricum
			break
		}
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
