package websocket

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

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
	hub := NewHub()

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

func originRequest(origin, host string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/ws", nil)
	r.Host = host
	if origin != "" {
		r.Header.Set("Origin", origin)
	}
	return r
}

func TestCheckOriginPolicy(t *testing.T) {
	hub := NewHub()
	hub.SetAllowedOrigins([]string{"https://rolink.network/", " HTTPS://Other.Example:8443 "})

	cases := []struct {
		name         string
		origin, host string
		want         bool
	}{
		{"no Origin (curl, tools)", "", "192.168.2.82:8088", true},
		{"same origin", "http://logs.lan:8080", "logs.lan:8080", true},
		{"same origin, different case", "http://LOGS.lan:8080", "logs.lan:8080", true},
		{"another site", "http://evil.example", "logs.lan:8080", false},
		{"same host, other port", "http://logs.lan:9999", "logs.lan:8080", false},
		{"invalid Origin", "://garbage", "logs.lan:8080", false},
		{"allow-list entry normalized (case, port, trailing slash)", "https://other.example:8443", "192.168.2.82:8088", true},
		{"behind proxy, a different site", "https://evil.example", "192.168.2.82:8088", false},
	}
	for _, c := range cases {
		if got := hub.checkOrigin(originRequest(c.origin, c.host)); got != c.want {
			t.Errorf("%s: origin %q host %q -> %v, want %v", c.name, c.origin, c.host, got, c.want)
		}
	}
}

func TestCheckOriginIsStrictByDefault(t *testing.T) {
	hub := NewHub()
	if hub.checkOrigin(originRequest("https://rolink.network", "192.168.2.82:8088")) {
		t.Error("with no allow-list, a proxied origin that differs from Host must be rejected")
	}
}

func TestCheckOriginWildcardAllowsEverything(t *testing.T) {
	hub := NewHub()
	hub.SetAllowedOrigins([]string{"*"})
	if !hub.checkOrigin(originRequest("http://evil.example", "logs.lan:8080")) {
		t.Error(`"*" is the explicit opt-in to accept any origin`)
	}
}

func TestWebSocketHandshakeHonoursTheAllowList(t *testing.T) {
	hub, srv := startHub(t)

	if _, resp, err := dial(t, srv, "https://rolink.network"); err == nil {
		t.Fatal("an origin that is not same-origin and not allow-listed must be rejected")
	} else if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403, got %v", resp)
	}

	hub.SetAllowedOrigins([]string{"https://rolink.network"})
	conn, _, err := dial(t, srv, "https://rolink.network")
	if err != nil {
		t.Fatalf("the allow-listed origin must connect: %v", err)
	}
	conn.Close()

	if _, _, err := dial(t, srv, "https://evil.example"); err == nil {
		t.Error("a site that is not in the list must still be rejected")
	}

	conn, _, err = dial(t, srv, srv.URL)
	if err != nil {
		t.Fatalf("same-origin must always connect: %v", err)
	}
	conn.Close()
}
