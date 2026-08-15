package web

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"syslog-server/storage"
	"syslog-server/types"
	"syslog-server/websocket"
)

var staticFiles embed.FS

const embeddedStaticRoot = "static"

type Server struct {
	wsHub   *websocket.Hub
	storage storage.Storage
	handler interface{}
}

func NewServer(wsHub *websocket.Hub, storage storage.Storage) *Server {
	return &Server{
		wsHub:   wsHub,
		storage: storage,
		handler: nil,
	}
}

func (s *Server) SetHandler(handler interface{}) {
	s.handler = handler
}

func (s *Server) SetupRoutes() {
	http.Handle("/", s.buildStaticHandler())
	http.HandleFunc("/ws", s.wsHub.ServeWebSocket)
	http.HandleFunc("/api/messages", s.handleGetMessages)
	http.HandleFunc("/api/messages/paginated", s.handleGetMessagesPaginated)
	http.HandleFunc("/api/messages/count", s.handleGetMessageCount)
	http.HandleFunc("/api/messages/search", s.handleSearchMessages)
	http.HandleFunc("/api/history", s.handleGetHistory)
	http.HandleFunc("/api/stats", s.handleGetStats)
}

func (s *Server) Start(addr string) error {
	s.SetupRoutes()
	return http.ListenAndServe(addr, nil)
}

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

func (s *Server) handleGetMessages(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	limitStr := r.URL.Query().Get("limit")
	limit := 100
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	if reader, ok := s.storage.(storage.Reader); ok {
		messages, err := reader.GetRecentMessages(limit)
		if err != nil {
			http.Error(w, `{"error": "Failed to read messages"}`, http.StatusInternalServerError)
			return
		}

		jsonData, err := json.Marshal(messages)
		if err != nil {
			http.Error(w, `{"error": "Failed to encode messages"}`, http.StatusInternalServerError)
			return
		}

		w.Write(jsonData)
	} else {
		http.Error(w, `{"error": "Storage does not support reading"}`, http.StatusNotImplemented)
	}
}

func (s *Server) handleGetStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	stats := make(map[string]interface{})

	if s.handler != nil {
		if h, ok := s.handler.(interface{ GetStats() map[string]interface{} }); ok {
			handlerStats := h.GetStats()
			for k, v := range handlerStats {
				stats[k] = v
			}
		}
	}

	if reader, ok := s.storage.(storage.Reader); ok {
		if count, err := reader.GetTotalMessageCount(); err == nil {
			stats["total_messages_stored"] = count
		}
	}

	jsonData, _ := json.Marshal(stats)
	w.Write(jsonData)
}

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

func (s *Server) handleSearchMessages(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

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
	if limit > 500 {
		limit = 500
	}
	if offset < 0 {
		offset = 0
	}

	severity := -1
	severityMax := -1

	if severityStr != "" {
		if strings.HasPrefix(severityStr, "range:") {
			rangeStr := strings.TrimPrefix(severityStr, "range:")
			parts := strings.Split(rangeStr, "-")
			if len(parts) == 2 {
				severity, _ = strconv.Atoi(parts[0])
				severityMax, _ = strconv.Atoi(parts[1])
			}
		} else {
			severityMap := map[string]int{
				"emergency": 0, "alert": 1, "critical": 2, "error": 3,
				"warning": 4, "notice": 5, "info": 6, "debug": 7,
			}
			if sev, ok := severityMap[severityStr]; ok {
				severity = sev
			}
		}
	}

	dateFromParsed := parseFlexibleTime(dateFrom, false)
	dateToParsed := parseFlexibleTime(dateTo, true)

	type Searcher interface {
		SearchMessages(searchTerm string, hostname string, severity int, severityMax int, dateFrom time.Time, dateTo time.Time, limit int, offset int) ([]*types.SyslogMessage, int, error)
	}

	searcher, ok := s.storage.(Searcher)
	if !ok {
		log.Printf("Storage type assertion failed. Storage type: %T", s.storage)
		http.Error(w, `{"error": "Storage does not support search"}`, http.StatusNotImplemented)
		return
	}

	messages, total, err := searcher.SearchMessages(searchTerm, hostname, severity, severityMax, dateFromParsed, dateToParsed, limit, offset)
	if err != nil {
		log.Printf("Search error: %v", err)
		http.Error(w, fmt.Sprintf(`{"error": "Search failed: %v"}`, err), http.StatusInternalServerError)
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
		http.Error(w, `{"error": "Failed to encode response"}`, http.StatusInternalServerError)
		return
	}

	w.Write(jsonData)
}

func (s *Server) handleGetMessagesPaginated(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

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
			http.Error(w, `{"error": "Failed to read messages"}`, http.StatusInternalServerError)
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
			http.Error(w, `{"error": "Failed to encode messages"}`, http.StatusInternalServerError)
			return
		}

		w.Write(jsonData)
	} else {
		http.Error(w, `{"error": "Storage does not support reading"}`, http.StatusNotImplemented)
	}
}

func (s *Server) handleGetMessageCount(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	if reader, ok := s.storage.(storage.Reader); ok {
		count, err := reader.GetTotalMessageCount()
		if err != nil {
			http.Error(w, `{"error": "Failed to get count"}`, http.StatusInternalServerError)
			return
		}

		response := map[string]int{"total": count}
		jsonData, _ := json.Marshal(response)
		w.Write(jsonData)
	} else {
		http.Error(w, `{"error": "Storage does not support counting"}`, http.StatusNotImplemented)
	}
}

func (s *Server) handleGetHistory(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	dateStr := r.URL.Query().Get("date")
	if dateStr == "" {
		dateStr = time.Now().Format("2006-01-02")
	}

	date, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		http.Error(w, `{"error": "Invalid date format"}`, http.StatusBadRequest)
		return
	}

	if reader, ok := s.storage.(storage.Reader); ok {
		messages, err := reader.GetMessagesByDate(date)
		if err != nil {
			http.Error(w, `{"error": "Failed to read messages for date"}`, http.StatusInternalServerError)
			return
		}

		jsonData, err := json.Marshal(messages)
		if err != nil {
			http.Error(w, `{"error": "Failed to encode messages"}`, http.StatusInternalServerError)
			return
		}

		w.Write(jsonData)
	} else {
		http.Error(w, `{"error": "Storage does not support reading"}`, http.StatusNotImplemented)
	}
}