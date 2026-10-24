package syslog

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var rmemMaxPath = "/proc/sys/net/core/rmem_max"

func readBufferCapWarning(requested int, path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	max, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil || requested <= max {
		return ""
	}
	return fmt.Sprintf("⚠️ UDP read buffer: asked for %d bytes but the kernel caps it at net.core.rmem_max=%d; "+
		"bursts will be dropped before the server can read them (RcvbufErrors in /proc/net/snmp). "+
		"Raise it with: sysctl -w net.core.rmem_max=%d (persist it in /etc/sysctl.d/)", requested, max, requested)
}

type ServerConfig struct {
	UDPReadBufferBytes int
	UDPQueueSize       int
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
		} else if warning := readBufferCapWarning(s.cfg.UDPReadBufferBytes, rmemMaxPath); warning != "" {
			log.Println(warning)
		}
	}

	queue := make(chan udpPacket, s.cfg.UDPQueueSize)

	s.wg.Add(3)
	go func() {
		defer s.wg.Done()
		<-ctx.Done()
		conn.Close()
	}()
	go func() {
		defer s.wg.Done()
		defer close(queue)
		s.udpReadLoop(ctx, conn, queue)
	}()
	go func() {
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
			time.Sleep(10 * time.Millisecond)
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
