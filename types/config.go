package types

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type Config struct {
	MySQL struct {
		Enabled  bool   `json:"enabled"`
		Host     string `json:"host"`
		Port     string `json:"port"`
		User     string `json:"user"`
		Password string `json:"password"`
		Database string `json:"database"`
	} `json:"mysql"`

	Storage struct {
		LogDirectory string `json:"log_directory"`
	} `json:"storage"`

	Ports struct {
		SyslogTCP string `json:"syslog_tcp"`
		SyslogUDP string `json:"syslog_udp"`
		Web       string `json:"web"`
	} `json:"ports"`

	RateLimiter struct {
		MessagesPerSecond int `json:"messages_per_second"`
		BurstSize         int `json:"burst_size"`
	} `json:"rate_limiter"`

	Alerting struct {
		Enabled          bool     `json:"enabled"`
		WebhookURL       string   `json:"webhook_url"`
		Severities       []string `json:"severities"`
		RateLimitMinutes int      `json:"rate_limit_minutes"`
		TimeoutSeconds   int      `json:"timeout_seconds"`
	} `json:"alerting"`

	Retention struct {
		Enabled bool   `json:"enabled"`
		Days    int    `json:"days"`
		RunAt   string `json:"run_at"` // Format: "HH:MM" (24h)
	} `json:"retention"`

	BanList struct {
		IPs       []string `json:"ips"`
		Hostnames []string `json:"hostnames"`
	} `json:"ban_list"`

	Filters struct {
		ExcludePatterns []string `json:"exclude_patterns"`
	} `json:"filters"`

	Dedup struct {
		Enabled            bool     `json:"enabled"`
		WindowSeconds      int      `json:"window_seconds"`
		MaxIntervalSeconds int      `json:"max_interval_seconds"`
		MaxEntries         int      `json:"max_entries"`
		NormalizePatterns  []string `json:"normalize_patterns"`
	} `json:"dedup"`

	Web struct {
		BasicAuthUser     string   `json:"basic_auth_user"`
		BasicAuthPassword string   `json:"basic_auth_password"`
		CORSAllowOrigin   string   `json:"cors_allow_origin"`
		AllowedOrigins    []string `json:"allowed_origins"`
	} `json:"web"`

	Syslog struct {
		UDPReadBufferBytes int  `json:"udp_read_buffer_bytes"`
		UDPQueueSize       int  `json:"udp_queue_size"`
		MaxTCPConnections  int  `json:"max_tcp_connections"`
		LogEveryMessage    bool `json:"log_every_message"`
	} `json:"syslog"`
}

func LoadConfig(configPath string) (*Config, bool, error) {
	config := &Config{}

	config.MySQL.Enabled = false
	config.MySQL.Host = "localhost"
	config.MySQL.Port = "3306"
	config.MySQL.User = "syslog"
	config.MySQL.Password = "password"
	config.MySQL.Database = "syslog"

	config.Storage.LogDirectory = "./logs"

	config.Ports.SyslogTCP = "0.0.0.0:514"
	config.Ports.SyslogUDP = "0.0.0.0:514"
	config.Ports.Web = ":8080"

	config.RateLimiter.MessagesPerSecond = 100
	config.RateLimiter.BurstSize = 200

	config.Alerting.Enabled = false
	config.Alerting.WebhookURL = ""
	config.Alerting.Severities = []string{"emergency", "critical"}
	config.Alerting.RateLimitMinutes = 60
	config.Alerting.TimeoutSeconds = 10

	config.Retention.Enabled = false
	config.Retention.Days = 90
	config.Retention.RunAt = "02:00"

	config.BanList.IPs = []string{}
	config.BanList.Hostnames = []string{}

	config.Filters.ExcludePatterns = []string{}

	config.Dedup.Enabled = true
	config.Dedup.WindowSeconds = 10
	config.Dedup.MaxIntervalSeconds = 60
	config.Dedup.MaxEntries = 10000
	config.Dedup.NormalizePatterns = []string{}

	config.Web.AllowedOrigins = []string{}

	config.Syslog.UDPReadBufferBytes = 4 * 1024 * 1024
	config.Syslog.UDPQueueSize = 8192
	config.Syslog.MaxTCPConnections = 1000
	config.Syslog.LogEveryMessage = false

	if configPath != "" {
		file, err := os.Open(configPath)
		if err != nil {
			if os.IsNotExist(err) {
				if writeErr := writeDefaultConfig(configPath, config); writeErr != nil {
					return config, false, nil
				}
				return config, true, nil
			}
			return nil, false, err
		}
		defer file.Close()

		decoder := json.NewDecoder(file)
		if err := decoder.Decode(config); err != nil {
			return nil, false, err
		}
	}

	return config, false, nil
}

func writeDefaultConfig(configPath string, config *Config) error {
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(configPath, data, 0600)
}

func (c *Config) Validate() []string {
	var warnings []string
	fix := func(cond bool, msg string, apply func()) {
		if cond {
			warnings = append(warnings, msg)
			apply()
		}
	}

	fix(c.RateLimiter.MessagesPerSecond < 1, "rate_limiter.messages_per_second < 1, using 100",
		func() { c.RateLimiter.MessagesPerSecond = 100 })
	fix(c.RateLimiter.BurstSize < c.RateLimiter.MessagesPerSecond,
		"rate_limiter.burst_size < messages_per_second, using 2x messages_per_second",
		func() { c.RateLimiter.BurstSize = c.RateLimiter.MessagesPerSecond * 2 })

	fix(c.Dedup.WindowSeconds < 1, "dedup.window_seconds < 1, using 10",
		func() { c.Dedup.WindowSeconds = 10 })
	fix(c.Dedup.MaxIntervalSeconds < c.Dedup.WindowSeconds,
		"dedup.max_interval_seconds < window_seconds, using max(60, window_seconds)",
		func() {
			c.Dedup.MaxIntervalSeconds = 60
			if c.Dedup.WindowSeconds > 60 {
				c.Dedup.MaxIntervalSeconds = c.Dedup.WindowSeconds
			}
		})
	fix(c.Dedup.MaxEntries < 1, "dedup.max_entries < 1, using 10000",
		func() { c.Dedup.MaxEntries = 10000 })

	fix(c.Syslog.UDPQueueSize < 1, "syslog.udp_queue_size < 1, using 8192",
		func() { c.Syslog.UDPQueueSize = 8192 })
	fix(c.Syslog.MaxTCPConnections < 1, "syslog.max_tcp_connections < 1, using 1000",
		func() { c.Syslog.MaxTCPConnections = 1000 })
	fix(c.Syslog.UDPReadBufferBytes < 0, "syslog.udp_read_buffer_bytes < 0, using 4 MiB",
		func() { c.Syslog.UDPReadBufferBytes = 4 * 1024 * 1024 })

	for _, origin := range c.Web.AllowedOrigins {
		if origin != "*" && !strings.Contains(origin, "://") {
			warnings = append(warnings, fmt.Sprintf("web.allowed_origins entry %q should look like https://host[:port]", origin))
		}
		if origin == "*" {
			warnings = append(warnings, "web.allowed_origins contains \"*\": any website can read the live logs through a visitor's browser")
		}
	}

	if c.Web.BasicAuthUser != "" && c.Web.BasicAuthPassword == "" {
		warnings = append(warnings, "web.basic_auth_user is set but web.basic_auth_password is empty")
	}

	return warnings
}
