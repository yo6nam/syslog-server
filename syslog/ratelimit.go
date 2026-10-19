package syslog

import (
	"sync"
	"time"
)

// RateLimiter implementează token bucket algorithm
type RateLimiter struct {
	// Per-IP limiters
	limiters map[string]*ipLimiter
	mutex    sync.RWMutex

	// Config
	messagesPerSecond int
	burstSize         int

	// Global stats
	totalDropped uint64
	statsMutex   sync.RWMutex
}

type ipLimiter struct {
	tokens    float64
	lastCheck time.Time
	dropped   uint64
}

// NewRateLimiter creează un rate limiter nou
// messagesPerSecond: cât de multe mesaje/sec sunt permise per IP
// burstSize: câte mesaje pot fi trimise dintr-odată (burst)
func NewRateLimiter(messagesPerSecond, burstSize int) *RateLimiter {
	rl := &RateLimiter{
		limiters:          make(map[string]*ipLimiter),
		messagesPerSecond: messagesPerSecond,
		burstSize:         burstSize,
	}

	// Cleanup periodic pentru IP-uri vechi
	go rl.cleanup()

	return rl
}

// Allow verifică dacă un mesaj de la un IP e permis
func (rl *RateLimiter) Allow(ip string) bool {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	limiter, exists := rl.limiters[ip]
	if !exists {
		limiter = &ipLimiter{
			tokens:    float64(rl.burstSize),
			lastCheck: time.Now(),
		}
		rl.limiters[ip] = limiter
	}

	now := time.Now()
	elapsed := now.Sub(limiter.lastCheck).Seconds()

	// Adaugă tokens bazat pe timp trecut
	limiter.tokens += elapsed * float64(rl.messagesPerSecond)
	if limiter.tokens > float64(rl.burstSize) {
		limiter.tokens = float64(rl.burstSize)
	}

	limiter.lastCheck = now

	// Verifică dacă avem un token disponibil
	if limiter.tokens >= 1.0 {
		limiter.tokens -= 1.0
		return true
	}

	// Rate limited!
	limiter.dropped++

	rl.statsMutex.Lock()
	rl.totalDropped++
	rl.statsMutex.Unlock()

	return false
}

// GetStats returnează statistici despre rate limiting
func (rl *RateLimiter) GetStats() map[string]interface{} {
	rl.mutex.RLock()
	defer rl.mutex.RUnlock()

	rl.statsMutex.RLock()
	totalDropped := rl.totalDropped
	rl.statsMutex.RUnlock()

	perIPStats := make(map[string]uint64)
	for ip, limiter := range rl.limiters {
		if limiter.dropped > 0 {
			perIPStats[ip] = limiter.dropped
		}
	}

	return map[string]interface{}{
		"total_dropped": totalDropped,
		"per_ip":        perIPStats,
		"tracked_ips":   len(rl.limiters),
	}
}

// cleanup șterge IP-urile care nu au mai trimis mesaje de mult timp
func (rl *RateLimiter) cleanup() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		rl.mutex.Lock()
		now := time.Now()

		for ip, limiter := range rl.limiters {
			// Șterge IP-urile inactive de peste 10 minute
			if now.Sub(limiter.lastCheck) > 10*time.Minute {
				delete(rl.limiters, ip)
			}
		}

		rl.mutex.Unlock()
	}
}
