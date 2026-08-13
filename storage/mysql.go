package storage

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
	"time"

	"syslog-server/types"
	_ "github.com/go-sql-driver/mysql"
)

type MySQLStorage struct {
	db          *sql.DB
	countCache  *CountCache
}

// CountCache pentru a evita COUNT(*) la fiecare request
type CountCache struct {
	count     int
	timestamp time.Time
	mutex     sync.RWMutex
	ttl       time.Duration
}

func NewCountCache(ttl time.Duration) *CountCache {
	return &CountCache{
		count:     0,
		timestamp: time.Time{},
		ttl:       ttl,
	}
}

func (cc *CountCache) Get() (int, bool) {
	cc.mutex.RLock()
	defer cc.mutex.RUnlock()

	if time.Since(cc.timestamp) > cc.ttl {
		return 0, false // Cache expired
	}
	return cc.count, true
}

func (cc *CountCache) Set(count int) {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()

	cc.count = count
	cc.timestamp = time.Now()
}

func (cc *CountCache) Increment() {
	cc.mutex.Lock()
	defer cc.mutex.Unlock()

	cc.count++
	cc.timestamp = time.Now()
}

type MySQLConfig struct {
	Host     string
	Port     string
	User     string
	Password string
	Database string
}

// validateDatabaseName verifică că numele database-ului e sigur
func validateDatabaseName(name string) error {
	if matched, _ := regexp.MatchString(`^[a-zA-Z0-9_]+$`, name); !matched {
		return fmt.Errorf("invalid database name: must contain only alphanumeric characters and underscores")
	}
	if len(name) == 0 || len(name) > 64 {
		return fmt.Errorf("invalid database name: length must be between 1 and 64 characters")
	}
	return nil
}

func NewMySQLStorage(config MySQLConfig) (*MySQLStorage, error) {
	if err := validateDatabaseName(config.Database); err != nil {
		return nil, err
	}

	dsnWithoutDB := fmt.Sprintf("%s:%s@tcp(%s:%s)/?parseTime=true",
		config.User, config.Password, config.Host, config.Port)

	// Attempt connection with RETRY (Retry loop de siguranță la reboot)
	maxRetries := 10
	var tempDB *sql.DB
	var err error

	for i := 1; i <= maxRetries; i++ {
		tempDB, err = sql.Open("mysql", dsnWithoutDB)
		if err == nil {
			// Încercăm un Ping fizic să fim siguri că MySQL acceptă conexiuni
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			err = tempDB.PingContext(ctx)
			cancel()
			if err == nil {
				break // Conexiune reușită!
			}
		}

		if tempDB != nil {
			tempDB.Close()
		}

		log.Printf("⚠️ MySQL not ready yet (attempt %d/%d): %v. Retrying in 5 seconds...", i, maxRetries, err)
		time.Sleep(5 * time.Second) // 👈 ESENȚIAL: Pauză pentru a elibera CPU-ul!
	}

	if err != nil {
		return nil, fmt.Errorf("failed to connect to MySQL after %d attempts: %v", maxRetries, err)
	}
	defer tempDB.Close()

	// 1. Creează baza de date dacă nu există
	createDBQuery := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", config.Database)
	if _, err := tempDB.Exec(createDBQuery); err != nil {
		return nil, fmt.Errorf("failed to create database: %v", err)
	}

	// 2. Conectare la baza de date specifică
	dsnWithDB := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true",
		config.User, config.Password, config.Host, config.Port, config.Database)

	db, err := sql.Open("mysql", dsnWithDB)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}

	// Connection Pool Settings
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// 3. Verifică tabelul
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS syslog_messages (
		id INT AUTO_INCREMENT PRIMARY KEY,
		timestamp DATETIME,
		hostname VARCHAR(255),
		tag VARCHAR(255),
		content TEXT,
		severity INT,
		facility INT,
		priority INT,
		remote_ip VARCHAR(45),
		protocol VARCHAR(10),
		raw_message TEXT,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		INDEX idx_timestamp (timestamp),
		INDEX idx_hostname (hostname),
		INDEX idx_severity (severity),
		INDEX idx_created_at (created_at),
		INDEX idx_protocol (protocol)
	)`

	if _, err := db.Exec(createTableSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create table: %v", err)
	}

	storage := &MySQLStorage{
		db:         db,
		countCache: NewCountCache(10 * time.Second),
	}

	// Initializare cache
	storage.refreshCountCache()

	log.Printf("✅ MySQL storage initialized: database '%s', table 'syslog_messages'", config.Database)
	return storage, nil
}

func (ms *MySQLStorage) refreshCountCache() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var count int
	query := `SELECT COUNT(*) FROM syslog_messages`
	if err := ms.db.QueryRowContext(ctx, query).Scan(&count); err == nil {
		ms.countCache.Set(count)
	}
}

func (ms *MySQLStorage) Store(message *types.SyslogMessage) error {
	query := `
	INSERT INTO syslog_messages
	(timestamp, hostname, tag, content, severity, facility, priority, remote_ip, protocol, raw_message)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := ms.db.Exec(query,
		message.Timestamp,
		message.Hostname,
		message.Tag,
		message.Content,
		message.Severity,
		message.Facility,
		message.Priority,
		message.RemoteIP,
		message.Protocol,
		message.RawMessage,
	)

	if err != nil {
		log.Printf("MySQL storage error: %v", err)
		return err
	}

	// Incrementează cache-ul în loc să faci COUNT(*)
	ms.countCache.Increment()

	return nil
}

func (ms *MySQLStorage) Close() error {
	log.Println("Closing MySQL connection...")
	return ms.db.Close()
}

func (ms *MySQLStorage) GetRecentMessages(limit int) ([]*types.SyslogMessage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
	SELECT id, timestamp, hostname, tag, content, severity, facility, priority, remote_ip, protocol, raw_message, created_at
	FROM syslog_messages
	ORDER BY id DESC
	LIMIT ?`

	rows, err := ms.db.QueryContext(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query recent messages: %v", err)
	}
	defer rows.Close()

	var messages []*types.SyslogMessage
	for rows.Next() {
		var msg types.SyslogMessage
		err := rows.Scan(
			&msg.ID,
			&msg.Timestamp,
			&msg.Hostname,
			&msg.Tag,
			&msg.Content,
			&msg.Severity,
			&msg.Facility,
			&msg.Priority,
			&msg.RemoteIP,
			&msg.Protocol,
			&msg.RawMessage,
			&msg.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan message: %v", err)
		}
		messages = append(messages, &msg)
	}

	return messages, nil
}

func (ms *MySQLStorage) GetMessagesByDate(date time.Time) ([]*types.SyslogMessage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	startOfDay := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, date.Location())
	endOfDay := startOfDay.Add(24 * time.Hour)

	query := `
	SELECT id, timestamp, hostname, tag, content, severity, facility, priority, remote_ip, protocol, raw_message, created_at
	FROM syslog_messages
	WHERE timestamp >= ? AND timestamp < ?
	ORDER BY id DESC`

	rows, err := ms.db.QueryContext(ctx, query, startOfDay, endOfDay)
	if err != nil {
		return nil, fmt.Errorf("failed to query messages by date: %v", err)
	}
	defer rows.Close()

	var messages []*types.SyslogMessage
	for rows.Next() {
		var msg types.SyslogMessage
		err := rows.Scan(
			&msg.ID,
			&msg.Timestamp,
			&msg.Hostname,
			&msg.Tag,
			&msg.Content,
			&msg.Severity,
			&msg.Facility,
			&msg.Priority,
			&msg.RemoteIP,
			&msg.Protocol,
			&msg.RawMessage,
			&msg.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan message: %v", err)
		}
		messages = append(messages, &msg)
	}

	return messages, nil
}

func (ms *MySQLStorage) GetMessagesPaginated(offset, limit int) ([]*types.SyslogMessage, int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Încearcă să obții count-ul din cache
	totalCount, cached := ms.countCache.Get()
	if !cached {
		// Cache miss - query database
		countQuery := `SELECT COUNT(*) FROM syslog_messages`
		if err := ms.db.QueryRowContext(ctx, countQuery).Scan(&totalCount); err != nil {
			return nil, 0, fmt.Errorf("failed to count messages: %v", err)
		}
		ms.countCache.Set(totalCount)
	}

	// Limitează offset pentru siguranță
	if offset < 0 {
		offset = 0
	}
	if limit > 1000 {
		limit = 1000
	}

	query := `
	SELECT id, timestamp, hostname, tag, content, severity, facility, priority, remote_ip, protocol, raw_message, created_at
	FROM syslog_messages
	ORDER BY id DESC
	LIMIT ? OFFSET ?`

	rows, err := ms.db.QueryContext(ctx, query, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to query paginated messages: %v", err)
	}
	defer rows.Close()

	var messages []*types.SyslogMessage
	for rows.Next() {
		var msg types.SyslogMessage
		err := rows.Scan(
			&msg.ID,
			&msg.Timestamp,
			&msg.Hostname,
			&msg.Tag,
			&msg.Content,
			&msg.Severity,
			&msg.Facility,
			&msg.Priority,
			&msg.RemoteIP,
			&msg.Protocol,
			&msg.RawMessage,
			&msg.CreatedAt,
		)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to scan message: %v", err)
		}
		messages = append(messages, &msg)
	}

	return messages, totalCount, nil
}

func (ms *MySQLStorage) GetTotalMessageCount() (int, error) {
	// Folosește cache-ul
	count, cached := ms.countCache.Get()
	if cached {
		return count, nil
	}

	// Cache miss - query database
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `SELECT COUNT(*) FROM syslog_messages`
	if err := ms.db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, fmt.Errorf("failed to get total message count: %v", err)
	}

	ms.countCache.Set(count)
	return count, nil
}

// SearchMessages caută în toate mesajele (server-side search)
func (ms *MySQLStorage) SearchMessages(searchTerm string, hostname string, severity int, severityMax int, dateFrom time.Time, dateTo time.Time, limit int, offset int) ([]*types.SyslogMessage, int, error) {
    ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
    defer cancel()

    // Build query CU PLACEHOLDERS, NU cu concatenare
    whereConditions := []string{}
    args := []interface{}{}

    if searchTerm != "" {
        whereConditions = append(whereConditions, "(content LIKE ? OR raw_message LIKE ?)")
        searchPattern := "%" + searchTerm + "%"
        args = append(args, searchPattern, searchPattern)
    }

    if hostname != "" {
        whereConditions = append(whereConditions, "hostname LIKE ?")
        args = append(args, "%"+hostname+"%")
    }

    if severity >= 0 && severityMax >= 0 {
        whereConditions = append(whereConditions, "severity BETWEEN ? AND ?")
        args = append(args, severity, severityMax)
    } else if severity >= 0 && severity <= 7 {
        whereConditions = append(whereConditions, "severity = ?")
        args = append(args, severity)
    }

    if !dateFrom.IsZero() && !dateTo.IsZero() {
        whereConditions = append(whereConditions, "timestamp BETWEEN ? AND ?")
        args = append(args, dateFrom, dateTo)
    } else if !dateFrom.IsZero() {
        whereConditions = append(whereConditions, "timestamp >= ?")
        args = append(args, dateFrom)
    } else if !dateTo.IsZero() {
        whereConditions = append(whereConditions, "timestamp <= ?")
        args = append(args, dateTo)
    }

    whereClause := ""
    if len(whereConditions) > 0 {
        whereClause = "WHERE " + strings.Join(whereConditions, " AND ")
    }

    // COUNT query - SAFE acum
    countQuery := "SELECT COUNT(*) FROM syslog_messages " + whereClause
    var totalCount int
    if err := ms.db.QueryRowContext(ctx, countQuery, args...).Scan(&totalCount); err != nil {
        return nil, 0, fmt.Errorf("failed to count search results: %v", err)
    }

    // Main query - SAFE acum
    query := fmt.Sprintf(`
        SELECT id, timestamp, hostname, tag, content, severity, facility, priority, remote_ip, protocol, raw_message, created_at
        FROM syslog_messages
        %s
        ORDER BY id DESC
        LIMIT ? OFFSET ?
    `, whereClause)

    // Adaugă limit/offset la args
    args = append(args, limit, offset)

    rows, err := ms.db.QueryContext(ctx, query, args...)
    if err != nil {
        return nil, 0, fmt.Errorf("failed to search messages: %v", err)
    }
    defer rows.Close()

    // Rest of the code remains the same...
    var messages []*types.SyslogMessage
    for rows.Next() {
        var msg types.SyslogMessage
        err := rows.Scan(
            &msg.ID, &msg.Timestamp, &msg.Hostname, &msg.Tag,
            &msg.Content, &msg.Severity, &msg.Facility, &msg.Priority,
            &msg.RemoteIP, &msg.Protocol, &msg.RawMessage, &msg.CreatedAt,
        )
        if err != nil {
            return nil, 0, fmt.Errorf("failed to scan message: %v", err)
        }
        messages = append(messages, &msg)
    }

    return messages, totalCount, nil
}

// DeleteOldMessages șterge mesajele mai vechi decât cutoffDate
func (ms *MySQLStorage) DeleteOldMessages(cutoffDate time.Time) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	query := `DELETE FROM syslog_messages WHERE timestamp < ?`

	result, err := ms.db.ExecContext(ctx, query, cutoffDate)
	if err != nil {
		return 0, fmt.Errorf("failed to delete old messages: %v", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %v", err)
	}

	// Invalidează cache-ul după ștergere
	ms.countCache.Set(0)
	ms.refreshCountCache()

	return int(rowsAffected), nil
}