package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

    "syslog-server/alerting"
	"syslog-server/syslog"
	"syslog-server/storage"
	"syslog-server/types"
	"syslog-server/websocket"
	"syslog-server/web"
)

func main() {
	configPath := flag.String("config", "config.json", "Path to configuration file")
	flag.Parse()

	config, err := types.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	log.Printf("Loaded configuration from: %s", *configPath)
	log.Printf("Log directory: %s", config.Storage.LogDirectory)
	log.Printf("MySQL enabled: %v", config.MySQL.Enabled)
	log.Printf("Rate Limiter: %d msg/sec, burst: %d", config.RateLimiter.MessagesPerSecond, config.RateLimiter.BurstSize)

	var storageBackend storage.Storage
	var mysqlStorage *storage.MySQLStorage

	fileStorage, err := storage.NewFileStorage(config.Storage.LogDirectory)
	if err != nil {
		log.Fatal("Failed to initialize file storage:", err)
	}

	if config.MySQL.Enabled {
		mysqlConfig := storage.MySQLConfig{
			Host:     getEnv("MYSQL_HOST", config.MySQL.Host),
			Port:     getEnv("MYSQL_PORT", config.MySQL.Port),
			User:     getEnv("MYSQL_USER", config.MySQL.User),
			Password: getEnv("MYSQL_PASSWORD", config.MySQL.Password),
			Database: getEnv("MYSQL_DATABASE", config.MySQL.Database),
		}

		log.Printf("Attempting MySQL connection to %s@%s:%s/%s",
			mysqlConfig.User, mysqlConfig.Host, mysqlConfig.Port, mysqlConfig.Database)

		mysqlStorage, err = storage.NewMySQLStorage(mysqlConfig)
		if err != nil {
			log.Printf("MySQL connection failed: %v", err)
			log.Printf("Using file storage only")
			storageBackend = fileStorage
		} else {
			log.Println("✅ MySQL storage connected successfully!")
			log.Println("✅ Using both MySQL and file storage")

			storageBackend = storage.NewMultiStorage(mysqlStorage, fileStorage)
		}
	} else {
		log.Println("MySQL storage disabled in configuration")
		storageBackend = fileStorage
	}

    // Create alerter
	alerter := alerting.NewWebhookAlerter(*config)

	wsHub := websocket.NewHub()
	go wsHub.Run()

	// Create handler with all config settings
	syslogHandler := syslog.NewHandler(
		storageBackend,
		wsHub,
		alerter,
		config.Filters.ExcludePatterns,
		config.BanList.IPs,
		config.BanList.Hostnames,
		config.RateLimiter.MessagesPerSecond,
		config.RateLimiter.BurstSize,
	)

	// Log ban list info
	if len(config.BanList.IPs) > 0 {
		log.Printf("🚫 Banned IPs: %d", len(config.BanList.IPs))
		for i, ip := range config.BanList.IPs {
			log.Printf("   %d. %s", i+1, ip)
		}
	}

	if len(config.BanList.Hostnames) > 0 {
		log.Printf("🚫 Banned Hostnames: %d", len(config.BanList.Hostnames))
		for i, hostname := range config.BanList.Hostnames {
			log.Printf("   %d. %s", i+1, hostname)
		}
	}

	// Log exclude filters
	if len(config.Filters.ExcludePatterns) > 0 {
		log.Printf("🚫 Exclude filters enabled: %d patterns", len(config.Filters.ExcludePatterns))
		for i, pattern := range config.Filters.ExcludePatterns {
			log.Printf("   %d. %s", i+1, pattern)
		}
	}

	// Start Web server
	webServer := web.NewServer(wsHub, storageBackend)
	webServer.SetHandler(syslogHandler) // Setează handler-ul pentru stats

	syslogServer := syslog.NewServer(syslogHandler)

	// Context pentru shutdown
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	defer shutdownCancel()

	var wg sync.WaitGroup

	// Start TCP listener cu shutdown
	var tcpListener net.Listener
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		tcpListener, err = syslogServer.StartTCPWithContext(shutdownCtx, config.Ports.SyslogTCP)
		if err != nil {
			log.Printf("TCP listener error: %v", err)
		}
	}()

	// Start UDP listener cu shutdown
	var udpConn *net.UDPConn
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		udpConn, err = syslogServer.StartUDPWithContext(shutdownCtx, config.Ports.SyslogUDP)
		if err != nil {
			log.Printf("UDP listener error: %v", err)
		}
	}()

	httpServer := &http.Server{
		Addr:    config.Ports.Web,
		Handler: http.DefaultServeMux,
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		log.Printf("Web interface available on %s", config.Ports.Web)

		if err := webServer.Start(config.Ports.Web); err != nil && err != http.ErrServerClosed {
			log.Printf("Web server error: %v", err)
		}
	}()

	log.Printf("Syslog server listening on %s (TCP/UDP)", config.Ports.SyslogTCP)
	log.Println("Server is ready and accepting connections!")

	// Ascultă pentru semnale de închidere
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down syslog server gracefully...")

	// 1. Cancelează context-ul pentru a opri acceptarea de conexiuni noi
	shutdownCancel()

	// 2. Închide listenerii pentru a opri imediat acceptarea conexiunilor
	if tcpListener != nil {
		tcpListener.Close()
		log.Println("TCP listener closed")
	}
	if udpConn != nil {
		udpConn.Close()
		log.Println("UDP listener closed")
	}

	// 3. Oprește web server-ul
	webShutdownCtx, webCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer webCancel()

	if err := httpServer.Shutdown(webShutdownCtx); err != nil {
		log.Printf("Web server shutdown error: %v", err)
	} else {
		log.Println("Web server closed")
	}

	// 4. Așteaptă ca goroutine-urile să se termine (dar nu prea mult)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Println("All listeners stopped")
	case <-time.After(3 * time.Second):
		log.Println("Listener shutdown timeout, continuing...")
	}

	// 5. ACUM închide storage-ul (după ce nu mai primim mesaje)
	log.Println("Closing storage connections...")
	if multiStorage, ok := storageBackend.(*storage.MultiStorage); ok {
		if err := multiStorage.Close(); err != nil {
			log.Printf("Error closing multi storage: %v", err)
		} else {
			log.Println("✅ Storage closed successfully")
		}
	} else if mysqlStorage != nil {
		if err := mysqlStorage.Close(); err != nil {
			log.Printf("Error closing MySQL storage: %v", err)
		} else {
			log.Println("✅ MySQL storage closed successfully")
		}
	}

	log.Println("Server shutdown complete")
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}