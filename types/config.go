package types

import (
	"encoding/json"
	"os"
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
}

func LoadConfig(configPath string) (*Config, bool, error) {
	config := &Config{}

	// Valori default
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

	// Default rate limiter
	config.RateLimiter.MessagesPerSecond = 100
	config.RateLimiter.BurstSize = 200

	// Default alerting
	config.Alerting.Enabled = false
	config.Alerting.WebhookURL = ""
	config.Alerting.Severities = []string{"emergency", "critical"}
	config.Alerting.RateLimitMinutes = 60
	config.Alerting.TimeoutSeconds = 10

	// ✅ Default retention
	config.Retention.Enabled = false
	config.Retention.Days = 90
	config.Retention.RunAt = "02:00"

	// Default ban list (gol)
	config.BanList.IPs = []string{}
	config.BanList.Hostnames = []string{}

	// Default filters
	config.Filters.ExcludePatterns = []string{}

	// Încarcă din fișier dacă există
	if configPath != "" {
		file, err := os.Open(configPath)
		if err != nil {
			// Dacă fișierul nu există, scrie configul default pe disc
			// (ca să existe ceva editabil) și continuă cu valorile default.
			if os.IsNotExist(err) {
				if writeErr := writeDefaultConfig(configPath, config); writeErr != nil {
					// Nu putem scrie fișierul (ex: director read-only) — continuăm
					// oricum cu valorile default, doar că nu vom avea un fișier creat.
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

// writeDefaultConfig scrie configul default (formatat, ușor de editat) la configPath.
func writeDefaultConfig(configPath string, config *Config) error {
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(configPath, data, 0644)
}