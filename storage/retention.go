package storage

import (
	"fmt"
	"log"
	"time"

	"syslog-server/types"
)

type RetentionManager struct {
	storage  Storage
	enabled  bool
	days     int
	runAt    string
	stopChan chan bool
}

func NewRetentionManager(storage Storage, config types.Config) *RetentionManager {
	if !config.Retention.Enabled {
		log.Println("📦 Retention policy disabled")
		return &RetentionManager{enabled: false}
	}

	// Validare days
	if config.Retention.Days < 1 {
		log.Printf("⚠️  Invalid retention days (%d), using default 90", config.Retention.Days)
		config.Retention.Days = 90
	}

	// Validare runAt format
	if !isValidTimeFormat(config.Retention.RunAt) {
		log.Printf("⚠️  Invalid run_at format (%s), using default 02:00", config.Retention.RunAt)
		config.Retention.RunAt = "02:00"
	}

	log.Printf("📦 Retention policy enabled: delete messages older than %d days", config.Retention.Days)
	log.Printf("🕐 Cleanup scheduled daily at %s", config.Retention.RunAt)

	return &RetentionManager{
		storage:  storage,
		enabled:  true,
		days:     config.Retention.Days,
		runAt:    config.Retention.RunAt,
		stopChan: make(chan bool),
	}
}

func isValidTimeFormat(timeStr string) bool {
	_, err := time.Parse("15:04", timeStr)
	return err == nil
}

// Start pornește scheduler-ul pentru cleanup
func (rm *RetentionManager) Start() {
	if !rm.enabled {
		return
	}

	go rm.scheduler()
	log.Println("✅ Retention manager started")
}

// Stop oprește scheduler-ul
func (rm *RetentionManager) Stop() {
	if !rm.enabled {
		return
	}

	rm.stopChan <- true
	log.Println("📦 Retention manager stopped")
}

func (rm *RetentionManager) scheduler() {
	// Rulează cleanup imediat la pornire (opțional)
	// rm.runCleanup()

	for {
		nextRun := rm.calculateNextRun()
		waitDuration := time.Until(nextRun)

		log.Printf("📅 Next cleanup scheduled at: %s (in %s)",
			nextRun.Format("2006-01-02 15:04:05"),
			formatDuration(waitDuration))

		select {
		case <-time.After(waitDuration):
			rm.runCleanup()
		case <-rm.stopChan:
			return
		}
	}
}

func (rm *RetentionManager) calculateNextRun() time.Time {
	now := time.Now()

	// Parse target time (ex: "02:00")
	targetTime, _ := time.Parse("15:04", rm.runAt)

	// Construiește data pentru astăzi la ora specificată
	nextRun := time.Date(
		now.Year(), now.Month(), now.Day(),
		targetTime.Hour(), targetTime.Minute(), 0, 0,
		now.Location(),
	)

	// Dacă ora a trecut deja azi, programează pentru mâine
	if nextRun.Before(now) {
		nextRun = nextRun.Add(24 * time.Hour)
	}

	return nextRun
}

func (rm *RetentionManager) runCleanup() {
	log.Println("🧹 Starting retention cleanup...")
	startTime := time.Now()

	cutoffDate := time.Now().AddDate(0, 0, -rm.days)

	deleted, err := rm.deleteOldMessages(cutoffDate)
	if err != nil {
		log.Printf("❌ Cleanup failed: %v", err)
		return
	}

	duration := time.Since(startTime)
	log.Printf("✅ Cleanup completed: deleted %d messages older than %s (took %s)",
		deleted,
		cutoffDate.Format("2006-01-02"),
		duration.Round(time.Millisecond))
}

func (rm *RetentionManager) deleteOldMessages(cutoffDate time.Time) (int, error) {
	// Verifică dacă storage-ul suportă ștergere
	if mysqlStorage, ok := rm.storage.(*MySQLStorage); ok {
		return mysqlStorage.DeleteOldMessages(cutoffDate)
	}

	if multiStorage, ok := rm.storage.(*MultiStorage); ok {
		// Dacă e MultiStorage, șterge din MySQL
		if multiStorage.mysql != nil {
			return multiStorage.mysql.DeleteOldMessages(cutoffDate)
		}
	}

	return 0, fmt.Errorf("storage backend does not support cleanup")
}

// ManualCleanup permite rularea manuală a cleanup-ului (pentru testing sau API)
func (rm *RetentionManager) ManualCleanup() (int, error) {
	if !rm.enabled {
		return 0, fmt.Errorf("retention policy is disabled")
	}

	cutoffDate := time.Now().AddDate(0, 0, -rm.days)
	return rm.deleteOldMessages(cutoffDate)
}

func formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	return fmt.Sprintf("%dh %dm", int(d.Hours()), int(d.Minutes())%60)
}