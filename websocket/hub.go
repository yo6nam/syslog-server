package websocket

import (
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = 30 * time.Second
	maxMessageSize = 4096
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

type Client struct {
	hub  *Hub
	conn *websocket.Conn
	send chan []byte
}

type Hub struct {
	clients    map[*Client]bool
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
	mutex      sync.RWMutex

	dropped     uint64 // mesaje pierdute pentru că canalul de broadcast era plin
	lastDropLog int64  // unix nano al ultimei linii de log despre drop-uri
}

func NewHub() *Hub {
	return &Hub{
		broadcast:  make(chan []byte, 256), // Buffered pentru a preveni blocking
		register:   make(chan *Client),
		unregister: make(chan *Client),
		clients:    make(map[*Client]bool),
	}
}

// ClientCount întoarce numărul de clienți conectați.
func (h *Hub) ClientCount() int {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	return len(h.clients)
}

// Dropped întoarce câte mesaje nu au putut fi puse în coada de broadcast.
func (h *Hub) Dropped() uint64 {
	return atomic.LoadUint64(&h.dropped)
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.mutex.Lock()
			h.clients[client] = true
			total := len(h.clients)
			h.mutex.Unlock()
			log.Printf("Client connected. Total clients: %d", total)

		case client := <-h.unregister:
			h.mutex.Lock()
			if _, ok := h.clients[client]; ok {
				delete(h.clients, client)
				close(client.send)
			}
			total := len(h.clients)
			h.mutex.Unlock()
			log.Printf("Client disconnected. Total clients: %d", total)

		case message := <-h.broadcast:
			h.mutex.Lock()
			for client := range h.clients {
				select {
				case client.send <- message:
				default:
					close(client.send)
					delete(h.clients, client)
					log.Printf("Client disconnected due to full buffer")
				}
			}
			h.mutex.Unlock()
		}
	}
}

func (h *Hub) Broadcast(message []byte) {
	// Non-blocking send pentru a preveni deadlock-uri
	select {
	case h.broadcast <- message:
	default:
		total := atomic.AddUint64(&h.dropped, 1)

		// O singură linie la cel mult 10s, nu una per mesaj pierdut.
		now := time.Now().UnixNano()
		last := atomic.LoadInt64(&h.lastDropLog)
		if now-last > int64(10*time.Second) && atomic.CompareAndSwapInt64(&h.lastDropLog, last, now) {
			log.Printf("Warning: Broadcast channel full, %d messages dropped so far", total)
		}
	}
}

func (h *Hub) ServeWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("WebSocket upgrade error:", err)
		return
	}

	client := &Client{
		hub:  h,
		conn: conn,
		send: make(chan []byte, 256),
	}

	h.register <- client

	// Start goroutines for this client
	go client.writePump()
	go client.readPump()
}

func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	// Ping/pong: fără ele, un client dispărut fără FIN (Wi-Fi căzut, laptop
	// închis) rămânea conectat până îi se umplea bufferul.
	c.conn.SetReadLimit(maxMessageSize)
	c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket error: %v", err)
			}
			break
		}
	}
}

func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// Hub closed the channel
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				log.Printf("WebSocket write error: %v", err)
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
