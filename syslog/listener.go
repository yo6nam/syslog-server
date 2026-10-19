package syslog

import (
	"bufio"
	"context"
	"errors"
	"log"
	"net"
	"sync"
	"time"
)

// ServerConfig reglează comportamentul listenerilor.
type ServerConfig struct {
	UDPReadBufferBytes int // SO_RCVBUF; 0 = valoarea implicită a sistemului
	UDPQueueSize       int // pachete așteptând procesarea
	MaxTCPConnections  int
}

func (c ServerConfig) withDefaults() ServerConfig {
	if c.UDPQueueSize <= 0 {
		c.UDPQueueSize = 8192
	}
	if c.MaxTCPConnections <= 0 {
		c.MaxTCPConnections = 1000
	}
	return c
}

type Server struct {
	handler *Handler
	cfg     ServerConfig

	wg sync.WaitGroup

	connsMu sync.Mutex
	conns   map[net.Conn]struct{}
}

func NewServer(handler *Handler) *Server {
	return NewServerWithConfig(handler, ServerConfig{})
}

func NewServerWithConfig(handler *Handler, cfg ServerConfig) *Server {
	return &Server{
		handler: handler,
		cfg:     cfg.withDefaults(),
		conns:   make(map[net.Conn]struct{}),
	}
}

// Wait așteaptă oprirea tuturor goroutine-urilor listenerilor (după anularea
// contextului), cel mult timeout. Întoarce false dacă s-a depășit timpul.
func (s *Server) Wait(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

// hostOnly extrage IP-ul dintr-o adresă "ip:port" (sau "[v6]:port"). Fără asta,
// ban-list-ul și rate limiter-ul vedeau pe TCP câte o "identitate" nouă per
// conexiune (portul efemer) și IP-ul se salva cu port.
func hostOnly(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	return host
}

func (s *Server) trackConn(conn net.Conn) {
	s.connsMu.Lock()
	s.conns[conn] = struct{}{}
	s.connsMu.Unlock()
}

func (s *Server) untrackConn(conn net.Conn) {
	s.connsMu.Lock()
	delete(s.conns, conn)
	s.connsMu.Unlock()
}

func (s *Server) closeAllConns() {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	for conn := range s.conns {
		conn.Close()
	}
}

// StartTCPWithContext deschide listenerul TCP și întoarce după bind (erorile de
// bind sunt returnate, nu fatale). Se oprește la anularea contextului.
func (s *Server) StartTCPWithContext(ctx context.Context, addr string) (net.Listener, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	s.wg.Add(2)
	go func() {
		defer s.wg.Done()
		<-ctx.Done()
		log.Println("TCP context cancelled, closing listener...")
		listener.Close()
		s.closeAllConns()
	}()
	go func() {
		defer s.wg.Done()
		s.acceptLoop(ctx, listener)
	}()

	log.Printf("TCP listener started on %s", addr)
	return listener, nil
}

func (s *Server) acceptLoop(ctx context.Context, listener net.Listener) {
	sem := make(chan struct{}, s.cfg.MaxTCPConnections)
	var connWG sync.WaitGroup
	defer connWG.Wait()

	var delay time.Duration
	var lastRejectLog time.Time

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			// Eroare persistentă (ex. EMFILE): backoff, nu buclă fierbinte.
			if delay == 0 {
				delay = 5 * time.Millisecond
			} else if delay *= 2; delay > time.Second {
				delay = time.Second
			}
			log.Printf("TCP accept error: %v (retrying in %v)", err, delay)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return
			}
			continue
		}
		delay = 0

		select {
		case sem <- struct{}{}:
		default:
			conn.Close()
			if time.Since(lastRejectLog) > 10*time.Second {
				lastRejectLog = time.Now()
				log.Printf("⚠️ TCP connection limit (%d) reached, rejecting new connections", s.cfg.MaxTCPConnections)
			}
			continue
		}

		connWG.Add(1)
		go func() {
			defer connWG.Done()
			defer func() { <-sem }()
			s.handleTCPConnection(conn)
		}()
	}
}

func (s *Server) handleTCPConnection(conn net.Conn) {
	s.trackConn(conn)
	defer func() {
		s.untrackConn(conn)
		conn.Close()
	}()

	// Keepalive detectează peer-urile dispărute fără să închidă conexiunile
	// legitime, doar idle.
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(60 * time.Second)
	}

	remoteIP := hostOnly(conn.RemoteAddr().String())

	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 0, 64*1024), 64*1024)

	for scanner.Scan() {
		message := scanner.Text()
		if message == "" {
			continue
		}
		s.handler.ProcessMessage(message, remoteIP, "TCP")
	}

	if err := scanner.Err(); err != nil && !errors.Is(err, net.ErrClosed) {
		log.Printf("TCP connection error from %s: %v", remoteIP, err)
	}
}

type udpPacket struct {
	data string
	ip   string
}

// StartUDPWithContext deschide socket-ul UDP. Citirea (rapidă) e separată de
// procesare printr-o coadă cu UN singur consumator: nu se mai pierd pachete în
// buffer-ul kernelului cât timp se scrie pe disc și ordinea mesajelor rămâne
// cea de sosire (înainte: un goroutine per pachet). Dacă și coada se umple,
// pierderea e numărată și vizibilă în statistici.
func (s *Server) StartUDPWithContext(ctx context.Context, addr string) (*net.UDPConn, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return nil, err
	}

	if s.cfg.UDPReadBufferBytes > 0 {
		if err := conn.SetReadBuffer(s.cfg.UDPReadBufferBytes); err != nil {
			log.Printf("⚠️ Could not set UDP read buffer to %d bytes: %v", s.cfg.UDPReadBufferBytes, err)
		}
	}

	queue := make(chan udpPacket, s.cfg.UDPQueueSize)

	s.wg.Add(3)
	go func() {
		defer s.wg.Done()
		<-ctx.Done()
		conn.Close()
	}()
	go func() { // cititor
		defer s.wg.Done()
		defer close(queue)
		s.udpReadLoop(ctx, conn, queue)
	}()
	go func() { // consumator unic
		defer s.wg.Done()
		for packet := range queue {
			s.handler.ProcessMessage(packet.data, packet.ip, "UDP")
		}
	}()

	log.Printf("UDP listener started on %s", addr)
	return conn, nil
}

func (s *Server) udpReadLoop(ctx context.Context, conn *net.UDPConn, queue chan<- udpPacket) {
	buffer := make([]byte, 65535)
	var lastDropLog time.Time

	for {
		n, remoteAddr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return
			}
			log.Printf("UDP read error: %v", err)
			time.Sleep(10 * time.Millisecond) // fără buclă fierbinte la erori persistente
			continue
		}

		packet := udpPacket{data: string(buffer[:n]), ip: remoteAddr.IP.String()}

		select {
		case queue <- packet:
		default:
			dropped := s.handler.CountQueueDrop()
			if time.Since(lastDropLog) > 10*time.Second {
				lastDropLog = time.Now()
				log.Printf("⚠️ UDP processing queue full: %d packets dropped so far (consider raising syslog.udp_queue_size)", dropped)
			}
		}
	}
}
