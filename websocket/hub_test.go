package websocket

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestCheckSameOrigin(t *testing.T) {
	cases := []struct {
		origin, host string
		want         bool
	}{
		{"", "logs.lan:8080", true},                      // unelte fără Origin
		{"http://logs.lan:8080", "logs.lan:8080", true},  // UI-ul propriu
		{"http://LOGS.lan:8080", "logs.lan:8080", true},  // case-insensitive
		{"http://evil.example", "logs.lan:8080", false},  // alt site
		{"http://logs.lan:9999", "logs.lan:8080", false}, // alt port
		{"://garbage", "logs.lan:8080", false},           // Origin invalid
	}
	for _, c := range cases {
		r := httptest.NewRequest(http.MethodGet, "/ws", nil)
		r.Host = c.host
		if c.origin != "" {
			r.Header.Set("Origin", c.origin)
		}
		if got := checkSameOrigin(r); got != c.want {
			t.Errorf("origin %q host %q: got %v, want %v", c.origin, c.host, got, c.want)
		}
	}
}

func startHub(t *testing.T) (*Hub, *httptest.Server) {
	t.Helper()
	hub := NewHub()
	go hub.Run()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", hub.ServeWebSocket)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return hub, srv
}

func dial(t *testing.T, srv *httptest.Server, origin string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	header := http.Header{}
	if origin != "" {
		header.Set("Origin", origin)
	}
	return websocket.DefaultDialer.Dial(url, header)
}

func waitClients(t *testing.T, hub *Hub, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if hub.ClientCount() == n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected %d clients, have %d", n, hub.ClientCount())
}

func TestBroadcastReachesConnectedClient(t *testing.T) {
	hub, srv := startHub(t)

	conn, _, err := dial(t, srv, "")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	waitClients(t, hub, 1)

	hub.Broadcast([]byte(`{"content":"hello"}`))

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, msg, err := conn.ReadMessage()
	if err != nil || string(msg) != `{"content":"hello"}` {
		t.Fatalf("got %q, %v", msg, err)
	}
}

func TestCrossSiteWebSocketIsRejected(t *testing.T) {
	_, srv := startHub(t)

	if _, resp, err := dial(t, srv, "http://evil.example"); err == nil {
		t.Fatal("a websocket opened from another site must be rejected")
	} else if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403, got %v", resp)
	}

	// Same-origin: trebuie să meargă.
	conn, _, err := dial(t, srv, srv.URL)
	if err != nil {
		t.Fatalf("same-origin connection must be accepted: %v", err)
	}
	conn.Close()
}

func TestClientIsUnregisteredOnDisconnect(t *testing.T) {
	hub, srv := startHub(t)

	conn, _, err := dial(t, srv, "")
	if err != nil {
		t.Fatal(err)
	}
	waitClients(t, hub, 1)

	conn.Close()
	waitClients(t, hub, 0)
}

func TestBroadcastDropsAreCountedNotBlocking(t *testing.T) {
	hub := NewHub() // Run() nu rulează: canalul de 256 se umple

	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			hub.Broadcast([]byte("x"))
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Broadcast must never block, even when the channel is full")
	}
	if hub.Dropped() != 1000-256 {
		t.Errorf("dropped = %d, want %d", hub.Dropped(), 1000-256)
	}
}
