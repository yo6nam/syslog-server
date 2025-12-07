package alerting

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"syslog-server/types"
)

type WebhookAlerter struct {
	enabled          bool
	webhookURL       string
	allowedSeverity  map[string]bool
	timeout          time.Duration

	// Rate limiting PER HOSTNAME
	lastSent         map[string]time.Time // hostname -> last sent time
	rateLimitMinutes int
	mutex            sync.RWMutex
}

type WebhookPayload struct {
	Timestamp  string `json:"timestamp"`
	Severity   string `json:"severity"`
	Hostname   string `json:"hostname"`
	Content    string `json:"content"`
	RemoteIP   string `json:"remote_ip"`
	RawMessage string `json:"raw_message"`
}

func NewWebhookAlerter(config types.Config) *WebhookAlerter {
	if !config.Alerting.Enabled || config.Alerting.WebhookURL == "" {
		return &WebhookAlerter{enabled: false}
	}

	severityMap := make(map[string]bool)
	for _, sev := range config.Alerting.Severities {
		severityMap[sev] = true
	}

	log.Printf("🔔 Webhook alerting enabled for: %v", config.Alerting.Severities)
	log.Printf("📡 Webhook URL: %s", config.Alerting.WebhookURL)
	log.Printf("⏱ Rate limit: %d minutes per hostname", config.Alerting.RateLimitMinutes)

	return &WebhookAlerter{
		enabled:          true,
		webhookURL:       config.Alerting.WebhookURL,
		allowedSeverity:  severityMap,
		timeout:          time.Duration(config.Alerting.TimeoutSeconds) * time.Second,
		lastSent:         make(map[string]time.Time),
		rateLimitMinutes: config.Alerting.RateLimitMinutes,
	}
}

func (wa *WebhookAlerter) SendAlert(message *types.SyslogMessage) {
	if !wa.enabled {
		return
	}

	severity := message.SeverityString()

	// Check if this severity should trigger alerts
	if !wa.allowedSeverity[severity] {
		return
	}

	// Check rate limit PER HOSTNAME
	hostname := message.Hostname
	if hostname == "" {
		hostname = message.RemoteIP // fallback la IP dacă n-ai hostname
	}

	if !wa.canSend(hostname) {
		log.Printf("⏸ Alert rate limited for host: %s (severity: %s)", hostname, severity)
		return
	}

	// Send async to not block message processing
	go wa.sendWebhook(message, severity)
}

func (wa *WebhookAlerter) canSend(hostname string) bool {
	wa.mutex.Lock()
	defer wa.mutex.Unlock()

	lastSent, exists := wa.lastSent[hostname]
	if !exists {
		wa.lastSent[hostname] = time.Now()
		return true
	}

	elapsed := time.Since(lastSent)
	rateLimit := time.Duration(wa.rateLimitMinutes) * time.Minute

	if elapsed < rateLimit {
		return false
	}

	wa.lastSent[hostname] = time.Now()
	return true
}

func (wa *WebhookAlerter) sendWebhook(message *types.SyslogMessage, severity string) {
	payload := WebhookPayload{
		Timestamp:  message.Timestamp.Format(time.RFC3339),
		Severity:   severity,
		Hostname:   message.Hostname,
		Content:    message.Content,
		RemoteIP:   message.RemoteIP,
		RawMessage: message.RawMessage,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		log.Printf("❌ Failed to marshal webhook payload: %v", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), wa.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", wa.webhookURL, bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("❌ Failed to create webhook request: %v", err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "SyslogServer/1.0")

	client := &http.Client{Timeout: wa.timeout}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("❌ Webhook request failed: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		log.Printf("✅ Alert sent [%s] from %s: %s",
			severity,
			message.Hostname,
			message.Content[:min(50, len(message.Content))])
	} else {
		log.Printf("⚠ Webhook returned status %d for %s/%s",
			resp.StatusCode,
			message.Hostname,
			severity)
	}
}

// StartCleanup curăță periodic cache-ul pentru a evita memory leak
func (wa *WebhookAlerter) StartCleanup() {
	if !wa.enabled {
		return
	}

	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()

		for range ticker.C {
			wa.mutex.Lock()
			now := time.Now()
			cutoff := time.Duration(wa.rateLimitMinutes*2) * time.Minute

			for hostname, lastSent := range wa.lastSent {
				if now.Sub(lastSent) > cutoff {
					delete(wa.lastSent, hostname)
				}
			}
			cleaned := len(wa.lastSent)
			wa.mutex.Unlock()

			log.Printf("🧹 Alert cache cleanup: %d hosts tracked", cleaned)
		}
	}()
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}