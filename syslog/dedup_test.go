package syslog

import (
	"strings"
	"sync"
	"testing"
	"time"

	"syslog-server/types"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

type collector struct {
	mu   sync.Mutex
	msgs []*types.SyslogMessage
}

func (c *collector) emit(m *types.SyslogMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.msgs = append(c.msgs, m)
}

func (c *collector) all() []*types.SyslogMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*types.SyslogMessage(nil), c.msgs...)
}

func newTestDedup(cfg DedupConfig) (*Deduplicator, *fakeClock, *collector) {
	clock := &fakeClock{t: time.Date(2026, 10, 18, 9, 53, 10, 0, time.UTC)}
	col := &collector{}
	d := NewDeduplicator(cfg, col.emit)
	d.now = clock.now
	return d, clock, col
}

func testMsg(host, tag, content string) *types.SyslogMessage {
	return &types.SyslogMessage{
		Hostname: host, Tag: tag, Content: content,
		Severity: 3, Facility: 0, RemoteIP: "10.0.0.5", Protocol: "UDP",
	}
}

func defaultCfg() DedupConfig {
	return DedupConfig{Window: 10 * time.Second, MaxInterval: 60 * time.Second, MaxEntries: 100}
}

const xradio = "xradio TXRX-ERR: [RX] IF=0, Receive failure: 4."

func TestFingerprintIgnoresKernelTimestamps(t *testing.T) {
	d, _, _ := newTestDedup(defaultCfg())

	// Fără tag de program, parser-ul pune timestamp-ul kernel în Tag...
	a := d.fingerprint(testMsg("yo6nam", "[404590.323487]", xradio))
	b := d.fingerprint(testMsg("yo6nam", "[404592.498636]", xradio))
	// ...iar cu "kernel:" îl pune în Content.
	c := d.fingerprint(testMsg("yo6nam", "kernel", "[404590.323487] "+xradio))
	e := d.fingerprint(testMsg("yo6nam", "kernel", "[ 12.000001]   "+xradio))

	if a != b {
		t.Error("kernel timestamp in tag must not change the fingerprint")
	}
	if c != e {
		t.Error("kernel timestamp in content must not change the fingerprint")
	}
}

func TestFingerprintKeepsMeaningfulDifferences(t *testing.T) {
	d, _, _ := newTestDedup(defaultCfg())
	base := d.fingerprint(testMsg("yo6nam", "", xradio))

	cases := map[string]*types.SyslogMessage{
		"different number": testMsg("yo6nam", "", "xradio TXRX-ERR: [RX] IF=0, Receive failure: 5."),
		"different host":   testMsg("other", "", xradio),
		"different tag":    testMsg("yo6nam", "sshd", xradio),
	}
	sev := testMsg("yo6nam", "", xradio)
	sev.Severity = 6
	cases["different severity"] = sev
	ip := testMsg("yo6nam", "", xradio)
	ip.RemoteIP = "10.0.0.6"
	cases["different source ip"] = ip

	for name, m := range cases {
		if d.fingerprint(m) == base {
			t.Errorf("%s must produce a different fingerprint", name)
		}
	}
}

func TestFirstPassesRepeatsSuppressedThenSummary(t *testing.T) {
	d, clock, col := newTestDedup(defaultCfg())

	if !d.Allow(testMsg("yo6nam", "[404590.323487]", xradio)) {
		t.Fatal("first occurrence must pass")
	}
	for i := 1; i <= 5; i++ {
		clock.advance(time.Second)
		if d.Allow(testMsg("yo6nam", "[40459"+string(rune('0'+i))+".1]", xradio)) {
			t.Fatalf("repeat #%d must be suppressed", i)
		}
	}

	d.tick() // încă în fereastră: nimic de emis
	if len(col.all()) != 0 {
		t.Fatal("no summary expected while the flood is still inside the window")
	}

	clock.advance(11 * time.Second)
	d.tick()

	got := col.all()
	if len(got) != 1 {
		t.Fatalf("expected exactly one summary, got %d", len(got))
	}
	want := xradio + " (repeated 5 times in 5s)"
	if got[0].Content != want {
		t.Errorf("summary content = %q, want %q", got[0].Content, want)
	}
	if got[0].Tag != "" {
		t.Errorf("kernel timestamp must be stripped from the summary tag, got %q", got[0].Tag)
	}
	if got[0].Hostname != "yo6nam" || got[0].Severity != 3 || got[0].RemoteIP != "10.0.0.5" {
		t.Errorf("summary must keep host/severity/ip of the original: %+v", got[0])
	}
	if d.Len() != 0 {
		t.Error("entry must be forgotten after the summary")
	}
	if !d.Allow(testMsg("yo6nam", "", xradio)) {
		t.Error("the same message after the quiet window must pass again")
	}
}

func TestSingularNoun(t *testing.T) {
	d, clock, col := newTestDedup(defaultCfg())
	d.Allow(testMsg("h", "", "boom"))
	clock.advance(time.Second)
	d.Allow(testMsg("h", "", "boom"))
	clock.advance(time.Minute)
	d.tick()

	got := col.all()
	if len(got) != 1 || !strings.Contains(got[0].Content, "(repeated 1 time in 1s)") {
		t.Fatalf("unexpected summary: %+v", got)
	}
}

func TestNoSummaryWhenNothingRepeated(t *testing.T) {
	d, clock, col := newTestDedup(defaultCfg())
	d.Allow(testMsg("h", "", "once"))
	clock.advance(time.Minute)
	d.tick()

	if len(col.all()) != 0 {
		t.Error("a message that never repeated must not produce a summary")
	}
	if d.Len() != 0 {
		t.Error("idle entries must be collected")
	}
}

func TestContinuousFloodEmitsPeriodicSummaries(t *testing.T) {
	d, clock, col := newTestDedup(defaultCfg())

	d.Allow(testMsg("h", "", xradio))
	for i := 1; i <= 130; i++ {
		clock.advance(time.Second)
		if d.Allow(testMsg("h", "", xradio)) {
			t.Fatalf("repeat at t+%ds must stay suppressed", i)
		}
	}

	got := col.all()
	if len(got) != 2 {
		t.Fatalf("expected 2 periodic summaries during the flood, got %d", len(got))
	}
	for i, m := range got {
		if !strings.Contains(m.Content, "repeated 60 times") {
			t.Errorf("summary %d = %q, want 60 repeats", i, m.Content)
		}
	}

	d.Stop() // golește restul de 10
	got = col.all()
	if len(got) != 3 || !strings.Contains(got[2].Content, "repeated 10 times") {
		t.Fatalf("Stop must flush the pending count, got %d summaries: %+v", len(got), got)
	}
}

func TestStaleEntryIsTreatedAsNewEvent(t *testing.T) {
	d, clock, col := newTestDedup(defaultCfg())

	d.Allow(testMsg("h", "", "flap"))
	clock.advance(time.Second)
	d.Allow(testMsg("h", "", "flap"))

	// Fereastra a expirat, dar tick-ul n-a rulat încă.
	clock.advance(30 * time.Second)
	if !d.Allow(testMsg("h", "", "flap")) {
		t.Fatal("after the quiet window the message is a new event and must pass")
	}
	if got := col.all(); len(got) != 1 || !strings.Contains(got[0].Content, "repeated 1 time") {
		t.Fatalf("the old count must be emitted before the new event: %+v", got)
	}

	clock.advance(time.Second)
	if d.Allow(testMsg("h", "", "flap")) {
		t.Error("the new event must start a fresh suppression window")
	}
}

func TestDifferentSourcesAreIndependent(t *testing.T) {
	d, _, _ := newTestDedup(defaultCfg())

	if !d.Allow(testMsg("hostA", "", "disk full")) || !d.Allow(testMsg("hostB", "", "disk full")) {
		t.Fatal("same text from different hosts must both pass")
	}
	if d.Allow(testMsg("hostA", "", "disk full")) {
		t.Error("repeat from hostA must be suppressed")
	}
}

func TestTableFullFailsOpen(t *testing.T) {
	cfg := defaultCfg()
	cfg.MaxEntries = 2
	d, _, _ := newTestDedup(cfg)

	d.Allow(testMsg("h", "", "A"))
	d.Allow(testMsg("h", "", "B"))

	if !d.Allow(testMsg("h", "", "C")) || !d.Allow(testMsg("h", "", "C")) {
		t.Error("when the table is full new messages must pass untracked (never lost)")
	}
	if d.Allow(testMsg("h", "", "A")) {
		t.Error("already tracked messages must still be suppressed")
	}
	if d.Len() != 2 {
		t.Errorf("table must not grow beyond MaxEntries, has %d", d.Len())
	}
}

func TestTableFullRecoversAfterWindow(t *testing.T) {
	cfg := defaultCfg()
	cfg.MaxEntries = 2
	d, clock, _ := newTestDedup(cfg)

	d.Allow(testMsg("h", "", "A"))
	d.Allow(testMsg("h", "", "B"))
	clock.advance(30 * time.Second)

	d.Allow(testMsg("h", "", "C")) // declanșează curățarea intrărilor expirate
	if d.Allow(testMsg("h", "", "C")) {
		t.Error("after expired entries are swept, C must be tracked and its repeat suppressed")
	}
}

func TestExtraNormalizePatterns(t *testing.T) {
	cfg := defaultCfg()
	cfg.NormalizePatterns = []string{`session \d+`, `(invalid`}
	d, _, _ := newTestDedup(cfg) // al doilea pattern e invalid și trebuie ignorat

	if !d.Allow(testMsg("h", "", "closed session 12345 by peer")) {
		t.Fatal("first must pass")
	}
	if d.Allow(testMsg("h", "", "closed session 67890 by peer")) == true {
		t.Error("messages differing only by a normalized pattern must be treated as repeats")
	}
}

func TestStopFlushesPendingSummaries(t *testing.T) {
	d, clock, col := newTestDedup(defaultCfg())

	for i := 0; i < 3; i++ {
		d.Allow(testMsg("h", "", "burst"))
		clock.advance(time.Second)
	}
	d.Stop()
	d.Stop() // idempotent

	got := col.all()
	if len(got) != 1 || !strings.Contains(got[0].Content, "repeated 2 times") {
		t.Fatalf("Stop must flush exactly one summary with the pending count: %+v", got)
	}
}

func TestRealTickerEmitsSummary(t *testing.T) {
	col := &collector{}
	d := NewDeduplicator(DedupConfig{Window: time.Second, MaxInterval: 5 * time.Second, MaxEntries: 10}, col.emit)
	d.Start()
	defer d.Stop()

	d.Allow(testMsg("h", "", "real time"))
	d.Allow(testMsg("h", "", "real time"))
	d.Allow(testMsg("h", "", "real time"))

	deadline := time.After(5 * time.Second)
	for {
		if got := col.all(); len(got) == 1 {
			if !strings.Contains(got[0].Content, "repeated 2 times") {
				t.Fatalf("unexpected summary: %q", got[0].Content)
			}
			return
		}
		select {
		case <-deadline:
			t.Fatal("no summary emitted by the background ticker within 5s")
		case <-time.After(100 * time.Millisecond):
		}
	}
}
