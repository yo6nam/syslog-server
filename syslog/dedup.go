package syslog

import (
	"fmt"
	"hash/fnv"
	"log"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"syslog-server/types"
)

// kernelTimestamp potrivește timestamp-urile de uptime ale kernel-ului, ex.
// "[404590.323487]" sau "[ 12.000001]". Diferă la fiecare linie, deci fără
// normalizare două mesaje identice ca sens nu s-ar potrivi niciodată.
var kernelTimestamp = regexp.MustCompile(`\[\s*\d+\.\d+\]`)

// DedupConfig controlează suprimarea mesajelor repetitive.
type DedupConfig struct {
	// Window: după cât timp fără repetări se emite rezumatul și se uită amprenta.
	Window time.Duration
	// MaxInterval: la flood continuu, rezumat cel puțin la acest interval.
	MaxInterval time.Duration
	// MaxEntries: limită de memorie pentru amprentele urmărite simultan.
	MaxEntries int
	// NormalizePatterns: regex-uri eliminate înainte de comparare, în plus
	// față de timestamp-urile kernel.
	NormalizePatterns []string
}

type dedupEntry struct {
	template    *types.SyslogMessage // primul mesaj (cel care a trecut)
	count       int                  // repetări suprimate de la ultimul rezumat
	windowStart time.Time            // începutul perioadei curente de numărare
	lastSeen    time.Time
}

// Deduplicator implementează "last message repeated N times": prima apariție
// a unui mesaj trece imediat; repetările identice (după normalizare) din
// fereastră sunt doar numărate, iar numărul e emis ca un singur rând rezumat.
type Deduplicator struct {
	cfg   DedupConfig
	extra []*regexp.Regexp
	emit  func(*types.SyslogMessage)
	now   func() time.Time // injectabil pentru teste

	mu        sync.Mutex
	entries   map[uint64]*dedupEntry
	lastSweep time.Time

	started  bool
	stopOnce sync.Once
	stop     chan struct{}
	done     chan struct{}
}

// NewDeduplicator creează deduplicatorul. emit primește rândurile rezumat
// (apelat fără lock-uri interne ținute).
func NewDeduplicator(cfg DedupConfig, emit func(*types.SyslogMessage)) *Deduplicator {
	if cfg.Window <= 0 {
		cfg.Window = 10 * time.Second
	}
	if cfg.MaxInterval < cfg.Window {
		cfg.MaxInterval = 60 * time.Second
		if cfg.Window > cfg.MaxInterval {
			cfg.MaxInterval = cfg.Window
		}
	}
	if cfg.MaxEntries <= 0 {
		cfg.MaxEntries = 10000
	}

	d := &Deduplicator{
		cfg:     cfg,
		emit:    emit,
		now:     time.Now,
		entries: make(map[uint64]*dedupEntry),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}

	for _, pattern := range cfg.NormalizePatterns {
		re, err := regexp.Compile(pattern)
		if err != nil {
			log.Printf("⚠️  dedup: ignoring invalid normalize pattern %q: %v", pattern, err)
			continue
		}
		d.extra = append(d.extra, re)
	}

	return d
}

func (d *Deduplicator) normalize(s string) string {
	s = kernelTimestamp.ReplaceAllString(s, " ")
	for _, re := range d.extra {
		s = re.ReplaceAllString(s, " ")
	}
	return strings.Join(strings.Fields(s), " ")
}

func (d *Deduplicator) fingerprint(m *types.SyslogMessage) uint64 {
	h := fnv.New64a()
	write := func(s string) {
		h.Write([]byte(s))
		h.Write([]byte{0})
	}
	write(m.RemoteIP)
	write(m.Hostname)
	write(fmt.Sprintf("%d/%d", m.Facility, m.Severity))
	write(d.normalize(m.Tag))
	write(d.normalize(m.Content))
	return h.Sum64()
}

// Allow întoarce true dacă mesajul trebuie procesat normal (stocat, difuzat,
// alertat) și false dacă e o repetare suprimată.
func (d *Deduplicator) Allow(m *types.SyslogMessage) bool {
	key := d.fingerprint(m)
	now := d.now()

	var summaries []*types.SyslogMessage
	pass := true

	d.mu.Lock()
	e, exists := d.entries[key]
	switch {
	case !exists:
		d.insertLocked(key, m, now)

	case now.Sub(e.lastSeen) >= d.cfg.Window:
		// Intrare expirată pe care tick-ul nu a apucat s-o colecteze încă:
		// o tratăm ca pe un eveniment nou, după ce emitem rezumatul vechi.
		if e.count > 0 {
			summaries = append(summaries, d.buildSummary(e, now))
		}
		d.entries[key] = &dedupEntry{template: m, windowStart: now, lastSeen: now}

	default:
		pass = false
		e.count++
		e.lastSeen = now
		if now.Sub(e.windowStart) >= d.cfg.MaxInterval {
			summaries = append(summaries, d.buildSummary(e, now))
			e.count = 0
			e.windowStart = now
		}
	}
	d.mu.Unlock()

	for _, s := range summaries {
		d.emit(s)
	}
	return pass
}

// insertLocked urmărește un mesaj nou. Dacă tabela e plină și nu se poate
// elibera nimic, mesajul pur și simplu nu e urmărit (fail-open: mai bine un
// duplicat în plus decât un mesaj pierdut).
func (d *Deduplicator) insertLocked(key uint64, m *types.SyslogMessage, now time.Time) {
	if len(d.entries) >= d.cfg.MaxEntries {
		// Curățare cel mult o dată pe secundă, ca un flood de mesaje unice
		// să nu transforme fiecare Allow într-un scan complet.
		if now.Sub(d.lastSweep) >= time.Second {
			d.lastSweep = now
			for k, e := range d.entries {
				if e.count == 0 && now.Sub(e.lastSeen) >= d.cfg.Window {
					delete(d.entries, k)
				}
			}
		}
		if len(d.entries) >= d.cfg.MaxEntries {
			return
		}
	}
	d.entries[key] = &dedupEntry{template: m, windowStart: now, lastSeen: now}
}

func (d *Deduplicator) buildSummary(e *dedupEntry, now time.Time) *types.SyslogMessage {
	t := e.template
	noun := "times"
	if e.count == 1 {
		noun = "time"
	}

	text := fmt.Sprintf("%s (repeated %d %s in %s)",
		d.normalize(t.Content), e.count, noun, formatSpan(e.lastSeen.Sub(e.windowStart)))

	return &types.SyslogMessage{
		Timestamp:  now,
		Hostname:   t.Hostname,
		Tag:        d.normalize(t.Tag),
		Content:    text,
		Severity:   t.Severity,
		Facility:   t.Facility,
		Priority:   t.Priority,
		RemoteIP:   t.RemoteIP,
		Protocol:   t.Protocol,
		RawMessage: text,
	}
}

func formatSpan(span time.Duration) string {
	if span < time.Second {
		return "<1s"
	}
	return span.Round(time.Second).String()
}

type pendingSummary struct {
	at  time.Time
	msg *types.SyslogMessage
}

// tick colectează intrările expirate și emite rezumatele care se cuvin.
func (d *Deduplicator) tick() {
	now := d.now()
	var pending []pendingSummary

	d.mu.Lock()
	for key, e := range d.entries {
		if now.Sub(e.lastSeen) >= d.cfg.Window {
			if e.count > 0 {
				pending = append(pending, pendingSummary{e.lastSeen, d.buildSummary(e, now)})
			}
			delete(d.entries, key)
		} else if e.count > 0 && now.Sub(e.windowStart) >= d.cfg.MaxInterval {
			pending = append(pending, pendingSummary{e.lastSeen, d.buildSummary(e, now)})
			e.count = 0
			e.windowStart = now
		}
	}
	d.mu.Unlock()

	d.emitSorted(pending)
}

func (d *Deduplicator) emitSorted(pending []pendingSummary) {
	sort.Slice(pending, func(i, j int) bool { return pending[i].at.Before(pending[j].at) })
	for _, p := range pending {
		d.emit(p.msg)
	}
}

// Start pornește bucla periodică (tick o dată pe secundă).
func (d *Deduplicator) Start() {
	d.mu.Lock()
	if d.started {
		d.mu.Unlock()
		return
	}
	d.started = true
	d.mu.Unlock()

	go func() {
		defer close(d.done)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				d.tick()
			case <-d.stop:
				return
			}
		}
	}()
}

// Stop oprește bucla și emite toate rezumatele în așteptare, ca numărătorile
// să nu se piardă la oprirea serverului.
func (d *Deduplicator) Stop() {
	d.stopOnce.Do(func() {
		close(d.stop)

		d.mu.Lock()
		started := d.started
		d.mu.Unlock()
		if started {
			<-d.done
		}

		now := d.now()
		var pending []pendingSummary

		d.mu.Lock()
		for key, e := range d.entries {
			if e.count > 0 {
				pending = append(pending, pendingSummary{e.lastSeen, d.buildSummary(e, now)})
			}
			delete(d.entries, key)
		}
		d.mu.Unlock()

		d.emitSorted(pending)
	})
}

// Len întoarce numărul de amprente urmărite (pentru statistici).
func (d *Deduplicator) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.entries)
}
