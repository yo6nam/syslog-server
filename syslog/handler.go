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
	excludePatterns []string // Patterns to exclude (as configured)
	excludeLower    []string // same patterns, lowercased once at construction
	bannedIPs       map[string]bool
	bannedHostnames map[string]bool

	dedup    *Deduplicator
	logEvery bool

	// Stats
	totalReceived     uint64
	totalDropped      uint64
	totalFiltered     uint64
	totalBanned       uint64
	totalSuppressed   uint64 // repetări suprimate de dedup
	totalSummaries    uint64 // rânduri rezumat emise de dedup
	totalQueueDropped uint64 // pachete UDP pierdute pentru că coada de procesare era plină
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

	excludeLower := make([]string, 0, len(excludePatterns))
	for _, pattern := range excludePatterns {
		excludeLower = append(excludeLower, strings.ToLower(pattern))
	}

	return &Handler{
		storage:         storage,
		wsHub:           wsHub,
		alerter:         alerter,
		excludePatterns: excludePatterns,
		excludeLower:    excludeLower,
		bannedIPs:       bannedIPsMap,
		bannedHostnames: bannedHostnamesMap,
		rateLimiter:     NewRateLimiter(messagesPerSec, burst),
	}
}

// EnableDedup activează suprimarea mesajelor repetitive. Trebuie apelat o
// singură dată, înainte de primirea traficului; Close() golește rezumatele.
func (h *Handler) EnableDedup(cfg DedupConfig) {
	h.dedup = NewDeduplicator(cfg, h.emitSummary)
	h.dedup.Start()
}

// SetLogEveryMessage controlează linia de log per mesaj primit.
func (h *Handler) SetLogEveryMessage(enabled bool) {
	h.logEvery = enabled
}

// CountQueueDrop e apelat de listener când coada UDP e plină.
func (h *Handler) CountQueueDrop() uint64 {
	return atomic.AddUint64(&h.totalQueueDropped, 1)
}

// Close golește starea în așteptare (rezumatele dedup) înainte de oprire.
func (h *Handler) Close() {
	if h.dedup != nil {
		h.dedup.Stop()
	}
}

// emitSummary stochează și difuzează un rând rezumat produs de dedup.
// Nu trece prin alertare: prima apariție a mesajului a alertat deja.
func (h *Handler) emitSummary(message *types.SyslogMessage) {
	atomic.AddUint64(&h.totalSummaries, 1)

	if err := h.storage.Store(message); err != nil {
		log.Printf("Error storing summary message: %v", err)
	}
	h.broadcastMessage(message)

	if h.logEvery {
		log.Printf("Summary from %s: %s", message.RemoteIP, message.Content)
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

	// Suprimă repetările identice (după normalizare); prima apariție trece.
	if h.dedup != nil && !h.dedup.Allow(message) {
		atomic.AddUint64(&h.totalSuppressed, 1)
		return
	}

	if h.alerter != nil {
		h.alerter.SendAlert(message)
	}

	if err := h.storage.Store(message); err != nil {
		log.Printf("Error storing message: %v", err)
	}

	h.broadcastMessage(message)

	if h.logEvery {
		log.Printf("Received syslog message from %s via %s: %s", remoteIP, protocol, message.Content)
	}
}

func (h *Handler) shouldExclude(content string) bool {
	if len(h.excludeLower) == 0 {
		return false
	}
	contentLower := strings.ToLower(content)
	for _, pattern := range h.excludeLower {
		if strings.Contains(contentLower, pattern) {
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

	dedupTracked := 0
	if h.dedup != nil {
		dedupTracked = h.dedup.Len()
	}

	return map[string]interface{}{
		"total_received":      atomic.LoadUint64(&h.totalReceived),
		"total_dropped":       atomic.LoadUint64(&h.totalDropped),
		"total_filtered":      atomic.LoadUint64(&h.totalFiltered),
		"total_banned":        atomic.LoadUint64(&h.totalBanned),
		"total_suppressed":    atomic.LoadUint64(&h.totalSuppressed),
		"total_summaries":     atomic.LoadUint64(&h.totalSummaries),
		"total_queue_dropped": atomic.LoadUint64(&h.totalQueueDropped),
		"dedup_enabled":       h.dedup != nil,
		"dedup_tracked":       dedupTracked,
		"rate_limiter":        rlStats,
		"exclude_filters":     len(h.excludePatterns),
		"banned_ips":          len(h.bannedIPs),
		"banned_hosts":        len(h.bannedHostnames),
	}
}
