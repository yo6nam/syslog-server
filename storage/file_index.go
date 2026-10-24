package storage

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"sort"

	"syslog-server/types"
)

// tailChunkSize e dimensiunea inițială citită de la sfârșitul unui fișier-zi
// la warmup (variabilă ca testele să o poată micșora).
var tailChunkSize int64 = 256 * 1024

// fileCount reține câte mesaje complete (linii terminate cu '\n') am numărat
// până la offset-ul size dintr-un fișier JSON.
type fileCount struct {
	size  int64
	count int
}

// totalFromIndex întoarce numărul total de mesaje de pe disc.
//
// Discul e singura sursă de adevăr: înainte, un contor din memorie era
// incrementat în Store() ȘI suprascris de un warmup asincron care citea tot de
// pe disc, deci un mesaj scris în timpul warmup-ului era numărat de două ori
// (și apărea dublat în recentCache). Aici nu mai există un al doilea contor care
// să diverge. Costul rămâne mic: fișierele deja numărate se sar (comparăm doar
// dimensiunea), iar la cele crescute (ziua curentă) se scanează doar octeții
// noi, numărând '\n' — fără parsare JSON.
func (fs *FileStorage) totalFromIndex() (int, error) {
	files, err := filepath.Glob(filepath.Join(fs.baseDir, "syslog-*.json"))
	if err != nil {
		return 0, err
	}

	fs.countMu.Lock()
	defer fs.countMu.Unlock()

	if fs.counts == nil {
		fs.counts = make(map[string]*fileCount)
	}

	seen := make(map[string]bool, len(files))
	total := 0

	for _, path := range files {
		info, err := os.Stat(path)
		if err != nil {
			continue // dispărut între Glob și Stat (retenție)
		}
		seen[path] = true

		fc := fs.counts[path]
		if fc == nil || info.Size() < fc.size { // fișier nou sau trunchiat: numărăm de la zero
			fc = &fileCount{}
			fs.counts[path] = fc
		}

		if info.Size() > fc.size {
			n, scanned, err := countNewlines(path, fc.size, info.Size())
			if err == nil {
				fc.count += n
				fc.size += scanned
			}
		}
		total += fc.count
	}

	for path := range fs.counts { // fișiere șterse de retenție
		if !seen[path] {
			delete(fs.counts, path)
		}
	}

	return total, nil
}

// countNewlines numără '\n' în [from, to) și întoarce câți octeți a citit.
// Fiecare mesaj e o linie JSON terminată cu '\n' (json.Encoder.Encode), iar
// newline-urile din interiorul textului sunt escapate, deci '\n' = un mesaj.
// O linie încă în curs de scriere nu are '\n' și e numărată la scanarea următoare.
func countNewlines(path string, from, to int64) (count int, scanned int64, err error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()

	if _, err := f.Seek(from, io.SeekStart); err != nil {
		return 0, 0, err
	}

	buf := make([]byte, 64*1024)
	remaining := to - from
	for remaining > 0 {
		chunk := buf
		if int64(len(chunk)) > remaining {
			chunk = chunk[:remaining]
		}
		n, readErr := f.Read(chunk)
		count += bytes.Count(chunk[:n], []byte{'\n'})
		scanned += int64(n)
		remaining -= int64(n)
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return count, scanned, readErr
		}
	}
	return count, scanned, nil
}

// warmupRecent umple recentCache cu cele mai recente mesaje, SINCRON (se
// termină înainte ca NewFileStorage să întoarcă obiectul, deci nu poate exista
// niciun Store() concurent) și rapid: citește doar coada fișierelor-zi cele mai
// noi, nu tot istoricul.
func (fs *FileStorage) warmupRecent() {
	messages, err := fs.loadRecentTail(fs.maxRecentCache)
	if err != nil {
		return
	}

	fs.cacheMutex.Lock()
	fs.recentCache = messages
	fs.cacheMutex.Unlock()
}

// loadRecentTail întoarce cele mai noi `limit` mesaje (cele mai noi primele),
// citind de la sfârșitul fișierelor, de la cel mai recent la cel mai vechi.
func (fs *FileStorage) loadRecentTail(limit int) ([]*types.SyslogMessage, error) {
	fs.readMutex.RLock()
	defer fs.readMutex.RUnlock()

	files, err := filepath.Glob(filepath.Join(fs.baseDir, "syslog-*.json"))
	if err != nil {
		return nil, err
	}
	sort.Sort(sort.Reverse(sort.StringSlice(files)))

	var collected []*types.SyslogMessage
	for _, path := range files {
		if len(collected) >= limit {
			break
		}
		messages, err := fs.readTail(path, limit-len(collected))
		if err != nil {
			continue
		}
		collected = append(collected, messages...)
	}

	sort.SliceStable(collected, func(i, j int) bool {
		return collected[i].Timestamp.After(collected[j].Timestamp)
	})
	if len(collected) > limit {
		collected = collected[:limit]
	}
	return collected, nil
}

// readTail citește ultimele maxMessages mesaje dintr-un fișier JSON-lines,
// mărind fereastra de la coadă până are destule linii complete sau ajunge la
// începutul fișierului.
func (fs *FileStorage) readTail(path string, maxMessages int) ([]*types.SyslogMessage, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()

	chunk := tailChunkSize
	for {
		start := size - chunk
		if start < 0 {
			start = 0
		}

		data := make([]byte, size-start)
		if _, err := f.ReadAt(data, start); err != nil && err != io.EOF {
			return nil, err
		}

		if start > 0 { // prima linie din fereastră e probabil tăiată la mijloc
			if i := bytes.IndexByte(data, '\n'); i >= 0 {
				data = data[i+1:]
			} else {
				data = nil
			}
		}

		var messages []*types.SyslogMessage
		for _, line := range bytes.Split(data, []byte{'\n'}) {
			if len(bytes.TrimSpace(line)) == 0 {
				continue
			}
			var raw map[string]interface{}
			if err := json.Unmarshal(line, &raw); err != nil {
				continue // linie coruptă sau încă în scriere
			}
			if msg, err := fs.parseMessageFromJSON(raw); err == nil {
				messages = append(messages, msg)
			}
		}

		if len(messages) >= maxMessages || start == 0 {
			if len(messages) > maxMessages {
				messages = messages[len(messages)-maxMessages:]
			}
			return messages, nil
		}
		chunk *= 2
	}
}
