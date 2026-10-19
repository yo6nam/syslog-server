package storage

import (
	"fmt"
	"sync"
	"time"

	"syslog-server/types"
)

type MultiStorage struct {
	backends []Storage
	mysql    *MySQLStorage
	file     *FileStorage
	mutex    sync.Mutex
}

func NewMultiStorage(backends ...Storage) *MultiStorage {
	ms := &MultiStorage{
		backends: backends,
	}

	for _, backend := range backends {
		if mysqlBackend, ok := backend.(*MySQLStorage); ok {
			ms.mysql = mysqlBackend
		}
		if fileBackend, ok := backend.(*FileStorage); ok {
			ms.file = fileBackend
		}
	}

	return ms
}

func (ms *MultiStorage) Store(message *types.SyslogMessage) error {
	var wg sync.WaitGroup
	errorChan := make(chan error, len(ms.backends))

	for _, backend := range ms.backends {
		wg.Add(1)
		go func(b Storage) {
			defer wg.Done()
			if err := b.Store(message); err != nil {
				errorChan <- err
			}
		}(backend)
	}

	wg.Wait()
	close(errorChan)

	// Colectează erorile
	var errors []error
	for err := range errorChan {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return errors[0]
	}
	return nil
}

// Pentru a putea închide conexiunile MySQL
func (ms *MultiStorage) Close() error {
	for _, backend := range ms.backends {
		if mysqlBackend, ok := backend.(*MySQLStorage); ok {
			if err := mysqlBackend.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}

// Implementare Reader interface - folosește primul backend care suportă citirea
func (ms *MultiStorage) GetRecentMessages(limit int) ([]*types.SyslogMessage, error) {
	for _, backend := range ms.backends {
		if reader, ok := backend.(Reader); ok {
			return reader.GetRecentMessages(limit)
		}
	}
	return nil, fmt.Errorf("no storage backend supports reading")
}

func (ms *MultiStorage) GetMessagesByDate(date time.Time) ([]*types.SyslogMessage, error) {
	for _, backend := range ms.backends {
		if reader, ok := backend.(Reader); ok {
			return reader.GetMessagesByDate(date)
		}
	}
	return nil, fmt.Errorf("no storage backend supports reading")
}

func (ms *MultiStorage) GetMessagesPaginated(offset, limit int) ([]*types.SyslogMessage, int, error) {
	for _, backend := range ms.backends {
		if reader, ok := backend.(Reader); ok {
			return reader.GetMessagesPaginated(offset, limit)
		}
	}
	return nil, 0, fmt.Errorf("no storage backend supports reading")
}

func (ms *MultiStorage) GetTotalMessageCount() (int, error) {
	for _, backend := range ms.backends {
		if reader, ok := backend.(Reader); ok {
			return reader.GetTotalMessageCount()
		}
	}
	return 0, fmt.Errorf("no storage backend supports counting")
}

// SearchMessages - folosește primul backend care suportă search
func (ms *MultiStorage) SearchMessages(searchTerm string, hostname string, severity int, severityMax int, dateFrom time.Time, dateTo time.Time, limit int, offset int) ([]*types.SyslogMessage, int, error) {
	type Searcher interface {
		SearchMessages(searchTerm string, hostname string, severity int, severityMax int, dateFrom time.Time, dateTo time.Time, limit int, offset int) ([]*types.SyslogMessage, int, error)
	}

	for _, backend := range ms.backends {
		if searcher, ok := backend.(Searcher); ok {
			return searcher.SearchMessages(searchTerm, hostname, severity, severityMax, dateFrom, dateTo, limit, offset)
		}
	}
	return nil, 0, fmt.Errorf("no storage backend supports search")
}
