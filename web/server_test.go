package web

import (
	"encoding/json"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"syslog-server/types"
	"syslog-server/websocket"
)

// fakeStore implementează Store + Reader + Searcher și reține parametrii
// primiți, ca testele să poată verifica plafoanele aplicate de handlere.
type fakeStore struct {
	total       int
	recentLimit int
	searchLimit int
}

func (f *fakeStore) Store(*types.SyslogMessage) error { return nil }

func (f *fakeStore) GetRecentMessages(limit int) ([]*types.SyslogMessage, error) {
	f.recentLimit = limit
	return []*types.SyslogMessage{}, nil
}

func (f *fakeStore) GetMessagesByDate(time.Time) ([]*types.SyslogMessage, error) {
	return []*types.SyslogMessage{}, nil
}

func (f *fakeStore) GetMessagesPaginated(offset, limit int) ([]*types.SyslogMessage, int, error) {
	return []*types.SyslogMessage{}, f.total, nil
}

func (f *fakeStore) GetTotalMessageCount() (int, error) { return f.total, nil }

func (f *fakeStore) SearchMessages(searchTerm string, hostname string, severity int, severityMax int, dateFrom time.Time, dateTo time.Time, limit int, offset int) ([]*types.SyslogMessage, int, error) {
	f.searchLimit = limit
	return []*types.SyslogMessage{}, f.total, nil
}

func newTestServer(store *fakeStore) *Server {
	return NewServer(websocket.NewHub(), store)
}

func get(t *testing.T, h http.Handler, target string, mutate func(*http.Request)) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	if mutate != nil {
		mutate(req)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// Garda împotriva dispariției directivei //go:embed (de ex. ștearsă de o unealtă
// de "cleanup" care o ia drept comentariu): fără ea, embed.FS compilează gol și
// UI-ul răspunde 404 la runtime, fără nicio eroare de compilare.
func TestEmbeddedUIAssetsArePresent(t *testing.T) {
	for _, name := range []string{
		"static/index.html",
		"static/styles.css",
		"static/js/app.js",
		"static/js/search.js",
		"static/js/websocket.js",
	} {
		data, err := fs.ReadFile(staticFiles, name)
		if err != nil || len(data) == 0 {
			t.Fatalf("%s missing from the embedded filesystem (is the //go:embed directive intact?): %v", name, err)
		}
	}
}

func TestIndexIsServedFromEmbeddedAssets(t *testing.T) {
	rec := get(t, newTestServer(&fakeStore{}).Handler(), "/", nil)

	if rec.Code != http.StatusOK {
		t.Fatalf("GET / = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "<html") {
		t.Error("GET / must return the real index.html")
	}
}

func TestSearchLimitIsCappedAt5000NotAt500(t *testing.T) {
	store := &fakeStore{}
	h := newTestServer(store).Handler()

	cases := map[string]int{
		"limit=5000":    5000, // regresie: handler-ul îl tăia la 500
		"limit=9999":    5000,
		"limit=500":     500,
		"limit=0":       100,
		"limit=garbage": 100,
		"":              100,
	}
	for query, want := range cases {
		store.searchLimit = -1
		rec := get(t, h, "/api/messages/search?q=x&"+query, nil)
		if rec.Code != http.StatusOK {
			t.Fatalf("%q -> status %d: %s", query, rec.Code, rec.Body.String())
		}
		if store.searchLimit != want {
			t.Errorf("%q: storage received limit %d, want %d", query, store.searchLimit, want)
		}
	}
}

func TestRecentMessagesLimitIsCapped(t *testing.T) {
	store := &fakeStore{}
	h := newTestServer(store).Handler()

	get(t, h, "/api/messages?limit=1000000", nil)
	if store.recentLimit != maxAPILimit {
		t.Errorf("limit forwarded to storage = %d, want cap %d", store.recentLimit, maxAPILimit)
	}

	get(t, h, "/api/messages", nil)
	if store.recentLimit != 100 {
		t.Errorf("default limit = %d, want 100", store.recentLimit)
	}
}

func TestPaginationReportsTotalForTheWarningBanner(t *testing.T) {
	h := newTestServer(&fakeStore{total: 12345}).Handler()

	rec := get(t, h, "/api/messages/search?q=x&limit=5000", nil)

	var body struct {
		Pagination struct {
			Total int `json:"total"`
			Limit int `json:"limit"`
		} `json:"pagination"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Pagination.Total != 12345 || body.Pagination.Limit != 5000 {
		t.Errorf("pagination = %+v, want total 12345 / limit 5000", body.Pagination)
	}
}

func TestBasicAuth(t *testing.T) {
	s := newTestServer(&fakeStore{})
	s.SetBasicAuth("admin", "s3cret")
	h := s.Handler()

	rec := get(t, h, "/api/stats", nil)
	if rec.Code != http.StatusUnauthorized || rec.Header().Get("WWW-Authenticate") == "" {
		t.Errorf("no credentials: status %d, WWW-Authenticate %q", rec.Code, rec.Header().Get("WWW-Authenticate"))
	}

	rec = get(t, h, "/api/stats", func(r *http.Request) { r.SetBasicAuth("admin", "wrong") })
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("wrong password: status %d, want 401", rec.Code)
	}

	rec = get(t, h, "/api/stats", func(r *http.Request) { r.SetBasicAuth("admin", "s3cret") })
	if rec.Code != http.StatusOK {
		t.Errorf("valid credentials: status %d, want 200", rec.Code)
	}

	// Protejează și UI-ul și websocket-ul, nu doar API-ul.
	if rec = get(t, h, "/", nil); rec.Code != http.StatusUnauthorized {
		t.Errorf("index must be protected, status %d", rec.Code)
	}
	if rec = get(t, h, "/ws", nil); rec.Code != http.StatusUnauthorized {
		t.Errorf("/ws must be protected, status %d", rec.Code)
	}
}

func TestNoAuthByDefault(t *testing.T) {
	rec := get(t, newTestServer(&fakeStore{}).Handler(), "/api/stats", nil)
	if rec.Code != http.StatusOK {
		t.Errorf("auth must be opt-in, status %d", rec.Code)
	}
}

func TestCORSIsOptIn(t *testing.T) {
	s := newTestServer(&fakeStore{})

	rec := get(t, s.Handler(), "/api/stats", nil)
	if v := rec.Header().Get("Access-Control-Allow-Origin"); v != "" {
		t.Errorf("default must send no CORS header (was %q)", v)
	}

	s2 := newTestServer(&fakeStore{})
	s2.SetCORSOrigin("https://dash.example")
	rec = get(t, s2.Handler(), "/api/stats", nil)
	if v := rec.Header().Get("Access-Control-Allow-Origin"); v != "https://dash.example" {
		t.Errorf("configured origin not echoed, got %q", v)
	}
}

func TestErrorsAreJSONWithCorrectContentType(t *testing.T) {
	rec := get(t, newTestServer(&fakeStore{}).Handler(), "/api/history?date=not-a-date", nil)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body["error"] == "" {
		t.Errorf("body must be a JSON error object, got %q (%v)", rec.Body.String(), err)
	}
}

func TestSetupRoutesIsIdempotent(t *testing.T) {
	s := newTestServer(&fakeStore{})
	s.SetupRoutes()
	s.SetupRoutes() // pe DefaultServeMux, al doilea apel dădea panică (pattern înregistrat de două ori)
	_ = s.Handler()
}

func TestParseFlexibleTime(t *testing.T) {
	if got := parseFlexibleTime("2026-10-18", false); got.Hour() != 0 {
		t.Errorf("date-only start must stay at midnight, got %v", got)
	}
	if got := parseFlexibleTime("2026-10-18", true); got.Hour() != 23 || got.Minute() != 59 {
		t.Errorf("date-only end must bump to end of day, got %v", got)
	}
	if got := parseFlexibleTime("2026-10-18T14:30:00Z", true); got.Hour() != 14 || got.Minute() != 30 {
		t.Errorf("a precise timestamp must stay untouched, got %v", got)
	}
	if !parseFlexibleTime("garbage", false).IsZero() || !parseFlexibleTime("", true).IsZero() {
		t.Error("unparseable or empty input must give the zero time")
	}
}
