package syslog

import (
	"bufio"
	"context"
	"io"
	"log"
	"net"
	"strings"
	"sync"
)

type Server struct {
	handler *Handler
}

func NewServer(handler *Handler) *Server {
	return &Server{
		handler: handler,
	}
}

// StartTCPWithContext pornește TCP listener cu suport pentru graceful shutdown
func (s *Server) StartTCPWithContext(ctx context.Context, addr string) (net.Listener, error) {
	if addr == ":514" {
		addr = "0.0.0.0:514"
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("TCP listener error:", err)
		return nil, err
	}

	log.Printf("TCP listener started on %s", addr)

	var wg sync.WaitGroup

	// Goroutine pentru a închide listener-ul când context-ul e cancelat
	go func() {
		<-ctx.Done()
		log.Println("TCP context cancelled, closing listener...")
		listener.Close()
		wg.Wait() // Așteaptă ca toate conexiunile să se termine
	}()

	// Loop principal pentru acceptare conexiuni
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				// Dacă context-ul e cancelat, nu mai logăm eroarea
				select {
				case <-ctx.Done():
					return
				default:
					log.Println("TCP accept error:", err)
					continue
				}
			}

			wg.Add(1)
			go func(c net.Conn) {
				defer wg.Done()
				s.handleTCPConnection(c)
			}(conn)
		}
	}()

	return listener, nil
}

// StartTCP - versiunea veche pentru backwards compatibility
func (s *Server) StartTCP(addr string) {
	s.StartTCPWithContext(context.Background(), addr)
	select {} // Block forever
}

// StartUDPWithContext pornește UDP listener cu suport pentru graceful shutdown
func (s *Server) StartUDPWithContext(ctx context.Context, addr string) (*net.UDPConn, error) {
	if addr == ":514" {
		addr = "0.0.0.0:514"
	}

	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		log.Fatal("UDP resolve error:", err)
		return nil, err
	}

	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Fatal("UDP listener error:", err)
		return nil, err
	}

	log.Printf("UDP listener started on %s", addr)

	// Goroutine pentru citire mesaje
	go func() {
		buffer := make([]byte, 65507) // Max UDP packet size
		for {
			select {
			case <-ctx.Done():
				log.Println("UDP context cancelled, stopping...")
				return
			default:
				n, addr, err := conn.ReadFromUDP(buffer)
				if err != nil {
					// Dacă context-ul e cancelat, oprim
					select {
					case <-ctx.Done():
						return
					default:
						// Altfel, doar logăm și continuăm
						if !strings.Contains(err.Error(), "use of closed network connection") {
							log.Println("UDP read error:", err)
						}
						continue
					}
				}

				message := string(buffer[:n])
				go s.handler.ProcessMessage(message, addr.IP.String(), "UDP")
			}
		}
	}()

	return conn, nil
}

// StartUDP - versiunea veche pentru backwards compatibility
func (s *Server) StartUDP(addr string) {
	s.StartUDPWithContext(context.Background(), addr)
	select {} // Block forever
}

func (s *Server) handleTCPConnection(conn net.Conn) {
	defer conn.Close()

	remoteAddr := conn.RemoteAddr().String()
	scanner := bufio.NewScanner(conn)

	// Setăm buffer size mai mare pentru mesaje lungi
	const maxMessageSize = 64 * 1024 // 64KB
	buf := make([]byte, maxMessageSize)
	scanner.Buffer(buf, maxMessageSize)

	for scanner.Scan() {
		message := strings.TrimSpace(scanner.Text())
		if message != "" {
			s.handler.ProcessMessage(message, remoteAddr, "TCP")
		}
	}

	if err := scanner.Err(); err != nil {
		if err != io.EOF && !strings.Contains(err.Error(), "use of closed network connection") {
			log.Printf("TCP scan error from %s: %v", remoteAddr, err)
		}
	}
}