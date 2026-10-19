package alerting

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"syslog-server/types"
)

type WebhookAlerter struct {
	enabled         bool
	webhookURL      string
	allowedSeverity map[string]bool
	timeout         time.Duration
	httpClient      *http.Client // reutilizat (keep-alive), nu unul nou per alertă

	// Rate limiting PER (HOSTNAME, SEVERITATE): o alertă "critical" banală nu
	// mai blochează o "emergency" reală de la aceeași gazdă.
	lastSent         map[string]time.Time // "hostname|severity" -> last sent time
	rateLimitMinutes int
	mutex            sync.RWMutex
	suppressed       uint64 // alerte oprite de rate limit (raportat la cleanup)
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
		// "Critical"/"CRITICAL" în config trebuie să funcționeze la fel ca
		// "critical"; înainte nu se potriveau niciodată, fără niciun avertisment.
		severityMap[strings.ToLower(strings.TrimSpace(sev))] = true
	}

	timeout := time.Duration(config.Alerting.TimeoutSeconds) * time.Second
	if timeout <= 0 {
		timeout = 10 * time.Second // 0 ar fi expirat instant toate cererile
	}

	log.Printf("🔔 Webhook alerting enabled for: %v", config.Alerting.Severities)
	log.Printf("📡 Webhook target: %s", maskURL(config.Alerting.WebhookURL))
	log.Printf("⏱ Rate limit: %d minutes per hostname+severity", config.Alerting.RateLimitMinutes)

	return &WebhookAlerter{
		enabled:          true,
		webhookURL:       config.Alerting.WebhookURL,
		allowedSeverity:  severityMap,
		timeout:          timeout,
		httpClient:       &http.Client{Timeout: timeout},
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

	if !wa.canSend(hostname + "|" + severity) {
		atomic.AddUint64(&wa.suppressed, 1)
		return
	}

	// Send async to not block message processing
	go wa.sendWebhook(message, severity)
}

func (wa *WebhookAlerter) canSend(key string) bool {
	wa.mutex.Lock()
	defer wa.mutex.Unlock()

	lastSent, exists := wa.lastSent[key]
	if !exists {
		wa.lastSent[key] = time.Now()
		return true
	}

	elapsed := time.Since(lastSent)
	rateLimit := time.Duration(wa.rateLimitMinutes) * time.Minute

	if elapsed < rateLimit {
		return false
	}

	wa.lastSent[key] = time.Now()
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

	resp, err := wa.httpClient.Do(req)
	if err != nil {
		log.Printf("❌ Webhook request failed: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		log.Printf("✅ Alert sent [%s] from %s: %s",
			severity,
			message.Hostname,
			truncate(message.Content, 50))
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

			for key, lastSent := range wa.lastSent {
				if now.Sub(lastSent) > cutoff {
					delete(wa.lastSent, key)
				}
			}
			cleaned := len(wa.lastSent)
			wa.mutex.Unlock()

			log.Printf("🧹 Alert cache cleanup: %d host/severity pairs tracked, %d alerts rate-limited since last cleanup",
				cleaned, atomic.SwapUint64(&wa.suppressed, 0))
		}
	}()
}

// truncate taie la maxRunes caractere (nu octeți — nu rupe UTF-8 la mijloc).
func truncate(s string, maxRunes int) string {
	runes := []rune(s)
	if len(runes) <= maxRunes {
		return s
	}
	return string(runes[:maxRunes])
}

// maskURL ascunde calea și query-ul unui URL (la webhook-uri Slack/Discord
// tocmai ele sunt secretul), păstrând doar schema și gazda pentru log.
func maskURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "<invalid url>"
	}
	return u.Scheme + "://" + u.Host + "/…"
}
