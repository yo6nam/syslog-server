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
