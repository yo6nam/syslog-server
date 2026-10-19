package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"syslog-server/alerting"
	"syslog-server/storage"
	"syslog-server/syslog"
	"syslog-server/types"
	"syslog-server/version"
	"syslog-server/web"
	"syslog-server/websocket"
)

func main() {
	configPath := flag.String("config", "config.json", "Path to configuration file")
	showVersionLong := flag.Bool("version", false, "Print version information and exit")
	showVersionShort := flag.Bool("v", false, "Print version information and exit (shorthand)")
	flag.Parse()

	if *showVersionLong || *showVersionShort {
		fmt.Println(version.String())
		os.Exit(0)
	}

	config, wroteDefault, err := types.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	if wroteDefault {
		log.Printf("No config file found at %s — created one with default values. Edit it and restart to customize.", *configPath)
	} else {
		log.Printf("Loaded configuration from: %s", *configPath)
	}
	for _, warning := range config.Validate() {
		log.Printf("⚠️  Config: %s", warning)
	}
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

	wsHub := websocket.NewHub()
	go wsHub.Run()

	// Create alerter
	alerter := alerting.NewWebhookAlerter(*config)
	alerter.StartCleanup()

	// ✅ Create retention manager
	retentionManager := storage.NewRetentionManager(storageBackend, *config)
	retentionManager.Start()

	// Create handler with alerter
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
	syslogHandler.SetLogEveryMessage(config.Syslog.LogEveryMessage)

	if config.Dedup.Enabled {
		syslogHandler.EnableDedup(syslog.DedupConfig{
			Window:            time.Duration(config.Dedup.WindowSeconds) * time.Second,
			MaxInterval:       time.Duration(config.Dedup.MaxIntervalSeconds) * time.Second,
			MaxEntries:        config.Dedup.MaxEntries,
			NormalizePatterns: config.Dedup.NormalizePatterns,
		})
		log.Printf("♻️  Duplicate suppression enabled: summary after %ds quiet, at least every %ds during a flood",
			config.Dedup.WindowSeconds, config.Dedup.MaxIntervalSeconds)
	} else {
		log.Println("Duplicate suppression disabled in configuration")
	}

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

	// Web server
	webServer := web.NewServer(wsHub, storageBackend)
	webServer.SetHandler(syslogHandler)
	webServer.SetBasicAuth(config.Web.BasicAuthUser, config.Web.BasicAuthPassword)
	webServer.SetCORSOrigin(config.Web.CORSAllowOrigin)

	if config.Web.BasicAuthUser != "" {
		log.Println("🔒 Web interface protected with HTTP Basic authentication")
	} else if !isLoopbackAddr(config.Ports.Web) {
		log.Printf("⚠️  Web interface on %s has no authentication (set web.basic_auth_user / web.basic_auth_password)", config.Ports.Web)
	}

	syslogServer := syslog.NewServerWithConfig(syslogHandler, syslog.ServerConfig{
		UDPReadBufferBytes: config.Syslog.UDPReadBufferBytes,
		UDPQueueSize:       config.Syslog.UDPQueueSize,
		MaxTCPConnections:  config.Syslog.MaxTCPConnections,
	})

	// Context pentru shutdown
	shutdownCtx, shutdownCancel := context.WithCancel(context.Background())
	defer shutdownCancel()

	// Listenerii se deschid sincron: erorile de bind sunt fatale la pornire, iar
	// "ready" nu mai apare înaintea socket-urilor (și dispar data race-urile pe
	// variabilele listenerilor).
	if _, err := syslogServer.StartTCPWithContext(shutdownCtx, config.Ports.SyslogTCP); err != nil {
		log.Fatalf("TCP listener error: %v", err)
	}
	if _, err := syslogServer.StartUDPWithContext(shutdownCtx, config.Ports.SyslogUDP); err != nil {
		log.Fatalf("UDP listener error: %v", err)
	}

	go func() {
		log.Printf("Web interface available on %s", config.Ports.Web)
		if err := webServer.Start(config.Ports.Web); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("Web server error: %v", err)
		}
	}()

	log.Printf("Syslog server listening on TCP %s, UDP %s", config.Ports.SyslogTCP, config.Ports.SyslogUDP)
	log.Println("Server is ready and accepting connections!")

	// Ascultă pentru semnale de închidere
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down syslog server gracefully...")

	// ✅ Stop retention manager
	retentionManager.Stop()

	// 1. Oprește listenerii (închide socket-urile și conexiunile TCP active)
	shutdownCancel()
	if syslogServer.Wait(5 * time.Second) {
		log.Println("All listeners stopped")
	} else {
		log.Println("Listener shutdown timeout, continuing...")
	}

	// 2. Golește starea în așteptare (rezumatele dedup) cât storage-ul e încă deschis
	syslogHandler.Close()

	// 3. Oprește web server-ul (de data asta chiar cel care rulează)
	webShutdownCtx, webCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer webCancel()

	if err := webServer.Shutdown(webShutdownCtx); err != nil {
		log.Printf("Web server shutdown error: %v", err)
	} else {
		log.Println("Web server closed")
	}

	// 4. ACUM închide storage-ul (după ce nu mai primim mesaje)
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

// isLoopbackAddr spune dacă adresa de ascultare e doar locală ("127.0.0.1:8080",
// "localhost:8080"); ":8080" înseamnă toate interfețele.
func isLoopbackAddr(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
