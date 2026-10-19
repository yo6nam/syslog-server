package web

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"embed"
	"encoding/json"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"syslog-server/storage"
	"syslog-server/types"
	"syslog-server/websocket"
)

// staticFiles conține tot conținutul directorului static/ înglobat direct în
// binar la compilare. Așa serverul funcționează standalone, fără să fie
// nevoie să cari folderul static/ alături de executabil.
//
//go:embed all:static
var staticFiles embed.FS

const embeddedStaticRoot = "static"

// maxAPILimit e plafonul de mesaje returnate de un singur request. Trebuie să
// rămână aliniat cu pagination.maxMessages din frontend (5000).
const maxAPILimit = 5000

type Server struct {
	wsHub   *websocket.Hub
	storage storage.Storage
	handler interface{} // Pentru a accesa GetStats() dacă există

	authUser   string
	authPass   string
	corsOrigin string

	routesOnce sync.Once
	mux        *http.ServeMux

	httpMu     sync.Mutex
	httpServer *http.Server
}

func NewServer(wsHub *websocket.Hub, storage storage.Storage) *Server {
	return &Server{
		wsHub:   wsHub,
		storage: storage,
		handler: nil, // Se poate seta mai târziu
	}
}

// SetHandler setează handler-ul pentru a avea acces la statistici
func (s *Server) SetHandler(handler interface{}) {
	s.handler = handler
}

// SetBasicAuth activează autentificarea HTTP Basic pentru UI, API și /ws.
// Un user gol o dezactivează.
func (s *Server) SetBasicAuth(user, password string) {
	s.authUser = user
	s.authPass = password
}

// SetCORSOrigin permite explicit un origin extern. Implicit nu se trimite
// niciun header CORS (UI-ul e servit de același server), în loc de
// "Access-Control-Allow-Origin: *" care lăsa orice site să citească logurile.
func (s *Server) SetCORSOrigin(origin string) {
	s.corsOrigin = origin
}

func (s *Server) setAPIHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	if s.corsOrigin != "" {
		w.Header().Set("Access-Control-Allow-Origin", s.corsOrigin)
	}
}

// jsonError scrie o eroare JSON cu Content-Type corect (http.Error îl forța pe
// text/plain deși corpul era JSON).
func jsonError(w http.ResponseWriter, message string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	body, _ := json.Marshal(map[string]string{"error": message})
	w.Write(body)
}

// SetupRoutes configurează rutele HTTP (pe un mux propriu, nu pe
// DefaultServeMux; apelabilă de mai multe ori fără panică).
func (s *Server) SetupRoutes() {
	s.routesOnce.Do(func() {
		mux := http.NewServeMux()
		mux.Handle("/", s.buildStaticHandler())
		mux.HandleFunc("/ws", s.wsHub.ServeWebSocket)
		mux.HandleFunc("/api/messages", s.handleGetMessages)
		mux.HandleFunc("/api/messages/paginated", s.handleGetMessagesPaginated)
		mux.HandleFunc("/api/messages/count", s.handleGetMessageCount)
		mux.HandleFunc("/api/messages/search", s.handleSearchMessages)
		mux.HandleFunc("/api/history", s.handleGetHistory)
		mux.HandleFunc("/api/stats", s.handleGetStats)
		s.mux = mux
	})
}

// Handler întoarce handler-ul HTTP complet (rute + autentificare).
func (s *Server) Handler() http.Handler {
	s.SetupRoutes()
	return s.withAuth(s.mux)
}

// withAuth cere HTTP Basic auth dacă a fost configurată.
func (s *Server) withAuth(next http.Handler) http.Handler {
	if s.authUser == "" {
		return next
	}

	// Se compară hash-uri, ca lungimea credențialelor să nu se scurgă prin timing.
	wantUser := sha256.Sum256([]byte(s.authUser))
	wantPass := sha256.Sum256([]byte(s.authPass))

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		gotUser := sha256.Sum256([]byte(user))
		gotPass := sha256.Sum256([]byte(pass))

		userOK := subtle.ConstantTimeCompare(gotUser[:], wantUser[:]) == 1
		passOK := subtle.ConstantTimeCompare(gotPass[:], wantPass[:]) == 1

		if !ok || !userOK || !passOK {
			w.Header().Set("WWW-Authenticate", `Basic realm="syslog-server"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Start pornește serverul HTTP (blochează). Întoarce http.ErrServerClosed după Shutdown.
func (s *Server) Start(addr string) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second, // fără timeouts: Slowloris ținea conexiunile deschise la nesfârșit
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	s.httpMu.Lock()
	s.httpServer = srv
	s.httpMu.Unlock()

	return srv.ListenAndServe()
}

// Shutdown oprește grațios serverul HTTP pornit cu Start.
func (s *Server) Shutdown(ctx context.Context) error {
	s.httpMu.Lock()
	srv := s.httpServer
	s.httpMu.Unlock()

	if srv == nil {
		return nil
	}
	return srv.Shutdown(ctx)
}

// buildStaticHandler alege sursa fișierelor UI:
//  1. Dacă există un director ./static pe disc (lângă binar) cu un index.html,
//     îl folosește — util pentru a personaliza UI-ul fără recompilare.
//  2. Altfel folosește fișierele înglobate în binar la compilare (implicit),
//     ceea ce face executabilul complet standalone.
func (s *Server) buildStaticHandler() http.Handler {
	if info, err := os.Stat("./static"); err == nil && info.IsDir() {
		if _, err := os.Stat(filepath.Join("./static", "index.html")); err == nil {
			log.Println("Serving static files from disk override: ./static")
			return http.FileServer(http.Dir("./static"))
		}
	}

	sub, err := fs.Sub(staticFiles, embeddedStaticRoot)
	if err != nil {
		log.Printf("Failed to load embedded static assets (%v) — serving empty ./static from disk", err)
		os.MkdirAll("./static", 0755)
		return http.FileServer(http.Dir("./static"))
	}

	log.Println("Serving static files from embedded assets (built into the binary)")
	return http.FileServer(http.FS(sub))
}

// handleGetMessages returnează mesaje recente
func (s *Server) handleGetMessages(w http.ResponseWriter, r *http.Request) {
	s.setAPIHeaders(w)

	// Obține parametrul limit din query string
	limitStr := r.URL.Query().Get("limit")
	limit := 100
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}
	if limit > maxAPILimit {
		limit = maxAPILimit // fără plafon, ?limit=1000000 citea tot istoricul în memorie
	}

	// Verifică dacă storage-ul suportă citirea
	if reader, ok := s.storage.(storage.Reader); ok {
		messages, err := reader.GetRecentMessages(limit)
		if err != nil {
			jsonError(w, "Failed to read messages", http.StatusInternalServerError)
			return
		}

		// Convert messages to JSON
		jsonData, err := json.Marshal(messages)
		if err != nil {
			jsonError(w, "Failed to encode messages", http.StatusInternalServerError)
			return
		}

		w.Write(jsonData)
	} else {
		jsonError(w, "Storage does not support reading", http.StatusNotImplemented)
	}
}

// handleGetStats returnează statistici despre server
func (s *Server) handleGetStats(w http.ResponseWriter, r *http.Request) {
	s.setAPIHeaders(w)

	stats := make(map[string]interface{})

	// Statistici din handler (dacă există)
	if s.handler != nil {
		if h, ok := s.handler.(interface{ GetStats() map[string]interface{} }); ok {
			handlerStats := h.GetStats()
			for k, v := range handlerStats {
				stats[k] = v
			}
		}
	}

	// Statistici despre conexiunile websocket
	stats["websocket_clients"] = s.wsHub.ClientCount()
	stats["websocket_broadcast_dropped"] = s.wsHub.Dropped()

	// Adaugă statistici despre storage
	if reader, ok := s.storage.(storage.Reader); ok {
		if count, err := reader.GetTotalMessageCount(); err == nil {
			stats["total_messages_stored"] = count
		}
	}

	jsonData, _ := json.Marshal(stats)
	w.Write(jsonData)
}

// parseFlexibleTime acceptă fie o dată simplă "2006-01-02" (din selectoarele
// de dată ale UI-ului), fie un timestamp precis RFC3339 (folosit de butonul
// "Last 24h", unde intervalul e o fereastră glisantă care nu se aliniază la
// miezul nopții). endOfDayIfDateOnly=true adaugă bump la sfârșitul zilei DOAR
// când input-ul e o dată simplă — un timestamp deja precis rămâne neatins.
func parseFlexibleTime(raw string, endOfDayIfDateOnly bool) time.Time {
	if raw == "" {
		return time.Time{}
	}

	preciseFormats := []string{
		time.RFC3339,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
	}
	for _, format := range preciseFormats {
		if t, err := time.Parse(format, raw); err == nil {
			return t
		}
	}

	if t, err := time.Parse("2006-01-02", raw); err == nil {
		if endOfDayIfDateOnly {
			t = t.Add(24*time.Hour - time.Second)
		}
		return t
	}

	return time.Time{}
}

// handleSearchMessages - server-side search în toate mesajele
func (s *Server) handleSearchMessages(w http.ResponseWriter, r *http.Request) {
	s.setAPIHeaders(w)

	// Parse parametri
	searchTerm := r.URL.Query().Get("q")
	hostname := r.URL.Query().Get("hostname")
	severityStr := r.URL.Query().Get("severity")
	dateFrom := r.URL.Query().Get("date_from")
	dateTo := r.URL.Query().Get("date_to")

	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))

	if limit <= 0 {
		limit = 100
	}
	if limit > maxAPILimit {
		limit = maxAPILimit
	}
	if offset < 0 {
		offset = 0
	}

	// Parse severity (support pentru range: range:0-4)
	severity := -1
	severityMax := -1

	if severityStr != "" {
		if strings.HasPrefix(severityStr, "range:") {
			// Parse range: "range:0-4" -> severity=0, severityMax=4
			rangeStr := strings.TrimPrefix(severityStr, "range:")
			parts := strings.Split(rangeStr, "-")
			if len(parts) == 2 {
				severity, _ = strconv.Atoi(parts[0])
				severityMax, _ = strconv.Atoi(parts[1])
			}
		} else {
			// Parse single severity
			severityMap := map[string]int{
				"emergency": 0, "alert": 1, "critical": 2, "error": 3,
				"warning": 4, "notice": 5, "info": 6, "debug": 7,
			}
			if sev, ok := severityMap[severityStr]; ok {
				severity = sev
			}
		}
	}

	// Parse dates — acceptă atât "2006-01-02" (din selectoarele de dată din
	// UI, pentru căutarea manuală), cât și un timestamp precis RFC3339
	// (folosit de butonul "Last 24h", unde intervalul nu se aliniază la
	// miezul nopții). Doar formatul dată-simplă primește bump la sfârșit de zi.
	dateFromParsed := parseFlexibleTime(dateFrom, false)
	dateToParsed := parseFlexibleTime(dateTo, true)

	// Verifică dacă storage suportă search
	type Searcher interface {
		SearchMessages(searchTerm string, hostname string, severity int, severityMax int, dateFrom time.Time, dateTo time.Time, limit int, offset int) ([]*types.SyslogMessage, int, error)
	}

	// Try direct assertion first
	searcher, ok := s.storage.(Searcher)
	if !ok {
		log.Printf("Storage type assertion failed. Storage type: %T", s.storage)
		jsonError(w, "Storage does not support search", http.StatusNotImplemented)
		return
	}

	messages, total, err := searcher.SearchMessages(searchTerm, hostname, severity, severityMax, dateFromParsed, dateToParsed, limit, offset)
	if err != nil {
		log.Printf("Search error: %v", err)
		log.Printf("Search failed: %v", err)
		jsonError(w, "Search failed", http.StatusInternalServerError)
		return
	}

	response := map[string]interface{}{
		"messages": messages,
		"pagination": map[string]interface{}{
			"offset": offset,
			"limit":  limit,
			"total":  total,
		},
		"search": map[string]interface{}{
			"term":      searchTerm,
			"hostname":  hostname,
			"severity":  severityStr,
			"date_from": dateFrom,
			"date_to":   dateTo,
		},
	}

	jsonData, err := json.Marshal(response)
	if err != nil {
		jsonError(w, "Failed to encode response", http.StatusInternalServerError)
		return
	}

	w.Write(jsonData)
}

// handleGetMessagesPaginated - nou endpoint pentru paginare
func (s *Server) handleGetMessagesPaginated(w http.ResponseWriter, r *http.Request) {
	s.setAPIHeaders(w)

	// Obține parametrii de paginare
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))

	if limit <= 0 {
		limit = 50
	}
	if limit > 1000 {
		limit = 1000
	}

	if offset < 0 {
		offset = 0
	}

	if reader, ok := s.storage.(storage.Reader); ok {
		messages, total, err := reader.GetMessagesPaginated(offset, limit)
		if err != nil {
			jsonError(w, "Failed to read messages", http.StatusInternalServerError)
			return
		}

		response := map[string]interface{}{
			"messages": messages,
			"pagination": map[string]int{
				"offset": offset,
				"limit":  limit,
				"total":  total,
			},
		}

		jsonData, err := json.Marshal(response)
		if err != nil {
			jsonError(w, "Failed to encode messages", http.StatusInternalServerError)
			return
		}

		w.Write(jsonData)
	} else {
		jsonError(w, "Storage does not support reading", http.StatusNotImplemented)
	}
}

// handleGetMessageCount - endpoint pentru numărul total
func (s *Server) handleGetMessageCount(w http.ResponseWriter, r *http.Request) {
	s.setAPIHeaders(w)

	if reader, ok := s.storage.(storage.Reader); ok {
		count, err := reader.GetTotalMessageCount()
		if err != nil {
			jsonError(w, "Failed to get count", http.StatusInternalServerError)
			return
		}

		response := map[string]int{"total": count}
		jsonData, _ := json.Marshal(response)
		w.Write(jsonData)
	} else {
		jsonError(w, "Storage does not support counting", http.StatusNotImplemented)
	}
}

// handleGetHistory returnează mesaje pentru o dată specificată
func (s *Server) handleGetHistory(w http.ResponseWriter, r *http.Request) {
	s.setAPIHeaders(w)

	dateStr := r.URL.Query().Get("date")
	if dateStr == "" {
		dateStr = time.Now().Format("2006-01-02")
	}

	date, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		jsonError(w, "Invalid date format", http.StatusBadRequest)
		return
	}

	if reader, ok := s.storage.(storage.Reader); ok {
		messages, err := reader.GetMessagesByDate(date)
		if err != nil {
			jsonError(w, "Failed to read messages for date", http.StatusInternalServerError)
			return
		}

		jsonData, err := json.Marshal(messages)
		if err != nil {
			jsonError(w, "Failed to encode messages", http.StatusInternalServerError)
			return
		}

		w.Write(jsonData)
	} else {
		jsonError(w, "Storage does not support reading", http.StatusNotImplemented)
	}
}
