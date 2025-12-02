package syslog

import (
	"encoding/json"
	"log"
	"strings"
	"sync/atomic"

	"syslog-server/alerting"
	"syslog-server/storage"
	"syslog-server/types"
	"syslog-server/websocket"
)

type Handler struct {
	storage         storage.Storage
	wsHub           *websocket.Hub
	rateLimiter     *RateLimiter
	alerter         *alerting.WebhookAlerter
	excludePatterns []string // Patterns to exclude
	bannedIPs       map[string]bool
	bannedHostnames map[string]bool

	// Stats
	totalReceived uint64
	totalDropped  uint64
	totalFiltered uint64
	totalBanned   uint64
}

func NewHandler(storage storage.Storage, wsHub *websocket.Hub, alerter *alerting.WebhookAlerter, excludePatterns []string, bannedIPs []string, bannedHostnames []string, messagesPerSec int, burst int) *Handler {
	// Create banned IPs map
	bannedIPsMap := make(map[string]bool)
	for _, ip := range bannedIPs {
		bannedIPsMap[strings.TrimSpace(ip)] = true
	}

	// Create banned hostnames map
	bannedHostnamesMap := make(map[string]bool)
	for _, hostname := range bannedHostnames {
		bannedHostnamesMap[strings.ToLower(strings.TrimSpace(hostname))] = true
	}

	return &Handler{
		storage:         storage,
		wsHub:           wsHub,
		alerter:         alerter,
		excludePatterns: excludePatterns,
		bannedIPs:       bannedIPsMap,
		bannedHostnames: bannedHostnamesMap,
		rateLimiter:     NewRateLimiter(messagesPerSec, burst),
	}
}

func (h *Handler) ProcessMessage(rawMessage string, remoteIP string, protocol string) {
	atomic.AddUint64(&h.totalReceived, 1)

	// Check ban list for IP
	if h.bannedIPs[remoteIP] {
		atomic.AddUint64(&h.totalBanned, 1)
		// Log doar la fiecare 100 de mesaje banned
		if atomic.LoadUint64(&h.totalBanned)%100 == 0 {
			log.Printf("🚫 Banned IP: dropped 100 messages from %s", remoteIP)
		}
		return
	}

	// Rate limiting check
	if !h.rateLimiter.Allow(remoteIP) {
		atomic.AddUint64(&h.totalDropped, 1)

		// Log doar la fiecare 100 de mesaje dropped
		if atomic.LoadUint64(&h.totalDropped)%100 == 0 {
			log.Printf("⚠️ Rate limit: dropped 100 messages from %s", remoteIP)
		}
		return
	}

	message, err := ParseMessage(rawMessage, remoteIP)
	if err != nil {
		log.Printf("Error parsing message: %v", err)
		return
	}

	message.Protocol = protocol

	// Check ban list for hostname
	if h.bannedHostnames[strings.ToLower(message.Hostname)] {
		atomic.AddUint64(&h.totalBanned, 1)
		// Log doar la fiecare 100 de mesaje banned
		if atomic.LoadUint64(&h.totalBanned)%100 == 0 {
			log.Printf("🚫 Banned hostname: dropped 100 messages from %s", message.Hostname)
		}
		return
	}

	// Check exclude patterns
	if h.shouldExclude(message.Content) {
		atomic.AddUint64(&h.totalFiltered, 1)
		// Log la fiecare 100 de mesaje filtrate
		if atomic.LoadUint64(&h.totalFiltered)%100 == 0 {
			log.Printf("🚫 Filtered: 100 messages matched exclude patterns")
		}
		return
	}

	if h.alerter != nil {
		h.alerter.SendAlert(message)
	}

	if err := h.storage.Store(message); err != nil {
		log.Printf("Error storing message: %v", err)
	}

	h.broadcastMessage(message)

	log.Printf("Received syslog message from %s via %s: %s", remoteIP, protocol, message.Content)
}

func (h *Handler) shouldExclude(content string) bool {
	contentLower := strings.ToLower(content)
	for _, pattern := range h.excludePatterns {
		if strings.Contains(contentLower, strings.ToLower(pattern)) {
			return true
		}
	}
	return false
}

func (h *Handler) broadcastMessage(message *types.SyslogMessage) {
	webMessage := map[string]interface{}{
		"timestamp":       message.Timestamp.Format("2006-01-02 15:04:05"),
		"hostname":        message.Hostname,
		"tag":             message.Tag,
		"content":         message.Content,
		"severity":        message.SeverityString(),
		"facility":        message.Facility,
		"facility_string": message.FacilityString(),
		"priority":        message.Priority,
		"remote_ip":       message.RemoteIP,
		"protocol":        message.Protocol,
		"raw_message":     message.RawMessage,
	}

	jsonData, err := json.Marshal(webMessage)
	if err != nil {
		log.Printf("Error marshaling message for WebSocket: %v", err)
		return
	}

	h.wsHub.Broadcast(jsonData)
}

// GetStats returnează statistici despre handler
func (h *Handler) GetStats() map[string]interface{} {
	rlStats := h.rateLimiter.GetStats()

	return map[string]interface{}{
		"total_received":  atomic.LoadUint64(&h.totalReceived),
		"total_dropped":   atomic.LoadUint64(&h.totalDropped),
		"total_filtered":  atomic.LoadUint64(&h.totalFiltered),
		"total_banned":    atomic.LoadUint64(&h.totalBanned),
		"rate_limiter":    rlStats,
		"exclude_filters": len(h.excludePatterns),
		"banned_ips":      len(h.bannedIPs),
		"banned_hosts":    len(h.bannedHostnames),
	}
}