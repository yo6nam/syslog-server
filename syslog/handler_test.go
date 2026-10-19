package syslog

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"syslog-server/types"
	"syslog-server/websocket"
)

type memStore struct {
	mu   sync.Mutex
	msgs []*types.SyslogMessage
}

func (m *memStore) Store(msg *types.SyslogMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.msgs = append(m.msgs, msg)
	return nil
}

func (m *memStore) snapshot() []*types.SyslogMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]*types.SyslogMessage(nil), m.msgs...)
}

func (m *memStore) waitFor(t *testing.T, n int, timeout time.Duration) []*types.SyslogMessage {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if got := m.snapshot(); len(got) >= n {
			return got
		}
		time.Sleep(10 * time.Millisecond)
	}
	got := m.snapshot()
	t.Fatalf("timed out waiting for %d stored messages, have %d", n, len(got))
	return got
}

func newTestHandler(store *memStore, excludes, bannedIPs, bannedHosts []string) *Handler {
	return NewHandler(store, websocket.NewHub(), nil, excludes, bannedIPs, bannedHosts, 1000000, 1000000)
}

func kernelLine(uptime string, failure int) string {
	return fmt.Sprintf("<131>Oct 18 09:53:10 yo6nam kernel: [%s] xradio TXRX-ERR: [RX] IF=0, Receive failure: %d.", uptime, failure)
}

func TestHandlerSuppressesRepetitiveKernelFlood(t *testing.T) {
	store := &memStore{}
	h := newTestHandler(store, nil, nil, nil)
	h.EnableDedup(DedupConfig{Window: 10 * time.Second, MaxInterval: time.Minute, MaxEntries: 100})

	for i := 0; i < 24; i++ {
		h.ProcessMessage(kernelLine(fmt.Sprintf("404590.%06d", 323487+i*1111), 4), "10.0.0.5", "UDP")
	}

	if got := store.snapshot(); len(got) != 1 {
		t.Fatalf("24 identical messages must store only the first, stored %d", len(got))
	}

	h.Close() // golește rezumatul

	got := store.snapshot()
	if len(got) != 2 {
		t.Fatalf("expected the original + one summary, got %d", len(got))
	}
	if !strings.Contains(got[1].Content, "repeated 23 times") {
		t.Errorf("summary = %q, want 23 repeats", got[1].Content)
	}

	stats := h.GetStats()
	if stats["total_received"].(uint64) != 24 || stats["total_suppressed"].(uint64) != 23 || stats["total_summaries"].(uint64) != 1 {
		t.Errorf("unexpected stats: %v", stats)
	}
}

func TestHandlerKeepsDistinctFailuresSeparate(t *testing.T) {
	store := &memStore{}
	h := newTestHandler(store, nil, nil, nil)
	h.EnableDedup(DedupConfig{Window: 10 * time.Second, MaxInterval: time.Minute, MaxEntries: 100})
	defer h.Close()

	h.ProcessMessage(kernelLine("404590.1", 4), "10.0.0.5", "UDP")
	h.ProcessMessage(kernelLine("404591.1", 5), "10.0.0.5", "UDP")

	if got := store.snapshot(); len(got) != 2 {
		t.Fatalf("different failure codes must not be merged, stored %d", len(got))
	}
}

func TestHandlerWithoutDedupStoresEverything(t *testing.T) {
	store := &memStore{}
	h := newTestHandler(store, nil, nil, nil)

	for i := 0; i < 10; i++ {
		h.ProcessMessage(kernelLine("404590.1", 4), "10.0.0.5", "UDP")
	}
	if got := store.snapshot(); len(got) != 10 {
		t.Fatalf("with dedup off every message must be stored, got %d", len(got))
	}
}

func TestHandlerExcludeIsCaseInsensitive(t *testing.T) {
	store := &memStore{}
	h := newTestHandler(store, []string{"XRADIO txrx-err"}, nil, nil)

	h.ProcessMessage(kernelLine("1.1", 4), "10.0.0.5", "UDP")
	h.ProcessMessage("<14>Oct 18 10:00:00 host app: something else", "10.0.0.5", "UDP")

	got := store.snapshot()
	if len(got) != 1 || !strings.Contains(got[0].Content, "something else") {
		t.Fatalf("exclude pattern must drop only the matching message: %+v", got)
	}
}

func startTestServer(t *testing.T, h *Handler, cfg ServerConfig) (*Server, context.CancelFunc, string, string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	srv := NewServerWithConfig(h, cfg)

	tcpLn, err := srv.StartTCPWithContext(ctx, "127.0.0.1:0")
	if err != nil {
		cancel()
		t.Fatalf("tcp listen: %v", err)
	}
	udpConn, err := srv.StartUDPWithContext(ctx, "127.0.0.1:0")
	if err != nil {
		cancel()
		t.Fatalf("udp listen: %v", err)
	}
	return srv, cancel, tcpLn.Addr().String(), udpConn.LocalAddr().String()
}

func TestTCPClientIPHasNoPort(t *testing.T) {
	store := &memStore{}
	h := newTestHandler(store, nil, nil, nil)
	srv, cancel, tcpAddr, _ := startTestServer(t, h, ServerConfig{})
	defer func() { cancel(); srv.Wait(2 * time.Second) }()

	conn, err := net.Dial("tcp", tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprint(conn, "<14>Oct 18 10:00:00 hostT app: via tcp\n")
	conn.Close()

	got := store.waitFor(t, 1, 3*time.Second)
	if got[0].RemoteIP != "127.0.0.1" {
		t.Errorf("RemoteIP = %q, want 127.0.0.1 (no ephemeral port)", got[0].RemoteIP)
	}
	if got[0].Protocol != "TCP" {
		t.Errorf("Protocol = %q, want TCP", got[0].Protocol)
	}
}

func TestBanListAppliesToTCP(t *testing.T) {
	store := &memStore{}
	h := newTestHandler(store, nil, []string{"127.0.0.1"}, nil)
	srv, cancel, tcpAddr, udpAddr := startTestServer(t, h, ServerConfig{})
	defer func() { cancel(); srv.Wait(2 * time.Second) }()

	tcp, err := net.Dial("tcp", tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprint(tcp, "<14>Oct 18 10:00:00 hostT app: via tcp\n")
	tcp.Close()

	udp, err := net.Dial("udp", udpAddr)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprint(udp, "<14>Oct 18 10:00:00 hostU app: via udp")
	udp.Close()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if h.GetStats()["total_banned"].(uint64) >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if banned := h.GetStats()["total_banned"].(uint64); banned != 2 {
		t.Errorf("both the TCP and the UDP message from a banned IP must be dropped, banned=%d", banned)
	}
	if got := store.snapshot(); len(got) != 0 {
		t.Errorf("banned IP must not reach storage on any protocol, stored %d", len(got))
	}
}

func TestUDPBurstKeepsOrderAndLosesNothing(t *testing.T) {
	const packets = 500 // încape și în buffer-ul implicit de 208 KB al unui runner CI

	store := &memStore{}
	h := newTestHandler(store, nil, nil, nil)
	srv, cancel, _, udpAddr := startTestServer(t, h, ServerConfig{UDPReadBufferBytes: 4 << 20, UDPQueueSize: 8192})
	defer func() { cancel(); srv.Wait(2 * time.Second) }()

	conn, err := net.Dial("udp", udpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for i := 0; i < packets; i++ {
		fmt.Fprintf(conn, "<14>Oct 18 10:00:00 hostU app: seq-%05d", i)
	}

	got := store.waitFor(t, packets, 5*time.Second)
	for i, m := range got[:packets] {
		want := fmt.Sprintf("seq-%05d", i)
		if m.Content != want {
			t.Fatalf("message %d out of order: got %q, want %q", i, m.Content, want)
		}
	}
	if dropped := h.GetStats()["total_queue_dropped"].(uint64); dropped != 0 {
		t.Errorf("no packet may be dropped by the processing queue, dropped=%d", dropped)
	}
}

func TestUDPQueueOverflowIsCounted(t *testing.T) {
	store := &memStore{}
	h := newTestHandler(store, nil, nil, nil)

	// Handler lent: blochează consumatorul ca să umplem coada.
	release := make(chan struct{})
	slow := &slowStore{inner: store, release: release}
	h.storage = slow

	srv, cancel, _, udpAddr := startTestServer(t, h, ServerConfig{UDPQueueSize: 4})
	defer func() { close(release); cancel(); srv.Wait(2 * time.Second) }()

	conn, err := net.Dial("udp", udpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for i := 0; i < 200; i++ {
		fmt.Fprintf(conn, "<14>Oct 18 10:00:00 hostU app: n-%d", i)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if h.GetStats()["total_queue_dropped"].(uint64) > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("a full processing queue must be reported in total_queue_dropped, not lost silently")
}

type slowStore struct {
	inner   *memStore
	release chan struct{}
}

func (s *slowStore) Store(m *types.SyslogMessage) error {
	<-s.release
	return s.inner.Store(m)
}

func TestServerShutdownStopsEverything(t *testing.T) {
	store := &memStore{}
	h := newTestHandler(store, nil, nil, nil)
	srv, cancel, tcpAddr, _ := startTestServer(t, h, ServerConfig{})

	// O conexiune TCP idle nu trebuie să blocheze oprirea.
	idle, err := net.Dial("tcp", tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer idle.Close()
	time.Sleep(50 * time.Millisecond)

	cancel()
	if !srv.Wait(3 * time.Second) {
		t.Fatal("listeners (including idle TCP connections) must stop after the context is cancelled")
	}
}

func TestHostOnly(t *testing.T) {
	cases := map[string]string{
		"127.0.0.1:45400":  "127.0.0.1",
		"[2001:db8::1]:51": "2001:db8::1",
		"10.1.2.3":         "10.1.2.3", // fără port: neschimbat
	}
	for in, want := range cases {
		if got := hostOnly(in); got != want {
			t.Errorf("hostOnly(%q) = %q, want %q", in, got, want)
		}
	}
}
