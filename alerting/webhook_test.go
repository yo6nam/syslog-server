package alerting

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"syslog-server/types"
)

func newAlerter(t *testing.T, url string) *WebhookAlerter {
	t.Helper()
	var cfg types.Config
	cfg.Alerting.Enabled = true
	cfg.Alerting.WebhookURL = url
	cfg.Alerting.Severities = []string{"CRITICAL", "EMERGENCY"}
	cfg.Alerting.RateLimitMinutes = 15
	cfg.Alerting.TimeoutSeconds = 0 // înainte: expira instant toate cererile
	return NewWebhookAlerter(cfg)
}

func msg(host string, severity int) *types.SyslogMessage {
	return &types.SyslogMessage{Hostname: host, Severity: severity, Content: "boom", Timestamp: time.Now()}
}

func waitCount(t *testing.T, c *int32, want int32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(c) >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected %d webhook calls, got %d", want, atomic.LoadInt32(c))
}

func TestRateLimitIsPerHostAndSeverity(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
	}))
	defer srv.Close()

	a := newAlerter(t, srv.URL)

	a.SendAlert(msg("hostA", 2)) // CRITICAL
	a.SendAlert(msg("hostA", 2)) // același host+severitate: limitat
	waitCount(t, &calls, 1)

	// O urgență reală de la aceeași gazdă nu mai e blocată de alerta critical.
	a.SendAlert(msg("hostA", 0)) // EMERGENCY
	waitCount(t, &calls, 2)

	a.SendAlert(msg("hostB", 2)) // altă gazdă
	waitCount(t, &calls, 3)

	time.Sleep(150 * time.Millisecond)
	if got := atomic.LoadInt32(&calls); got != 3 {
		t.Errorf("webhook called %d times, want exactly 3", got)
	}
}

func TestSeveritiesNotConfiguredAreIgnored(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
	}))
	defer srv.Close()

	newAlerter(t, srv.URL).SendAlert(msg("hostA", 6)) // INFO
	time.Sleep(150 * time.Millisecond)
	if atomic.LoadInt32(&calls) != 0 {
		t.Error("INFO is not configured and must not alert")
	}
}

func TestMaskURLHidesSecretPath(t *testing.T) {
	got := maskURL("https://hooks.slack.com/services/T000/B000/SECRETTOKEN?x=1")
	if strings.Contains(got, "SECRET") || strings.Contains(got, "services") {
		t.Errorf("secret leaked into log output: %q", got)
	}
	if !strings.Contains(got, "hooks.slack.com") {
		t.Errorf("host should stay visible for diagnostics: %q", got)
	}
	if maskURL("not a url") != "<invalid url>" {
		t.Error("invalid URL must not be echoed")
	}
}

func TestTruncateDoesNotBreakUTF8(t *testing.T) {
	in := strings.Repeat("ă", 80)
	out := truncate(in, 50)
	if len([]rune(out)) != 50 || strings.ContainsRune(out, '\uFFFD') {
		t.Errorf("truncate must cut on rune boundaries, got %d runes: %q", len([]rune(out)), out)
	}
	if truncate("short", 50) != "short" {
		t.Error("short strings must be unchanged")
	}
}
