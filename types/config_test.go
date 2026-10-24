package types

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestLoadConfigMissingFileWritesDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")

	cfg, created, err := LoadConfig(path)
	if err != nil || !created {
		t.Fatalf("created=%v err=%v, want a freshly written default file", created, err)
	}
	if !cfg.Dedup.Enabled || cfg.Dedup.WindowSeconds != 10 || cfg.Dedup.MaxIntervalSeconds != 60 {
		t.Errorf("unexpected dedup defaults: %+v", cfg.Dedup)
	}
	if cfg.Syslog.UDPReadBufferBytes != 4*1024*1024 || cfg.Syslog.UDPQueueSize != 8192 {
		t.Errorf("unexpected syslog defaults: %+v", cfg.Syslog)
	}
	if cfg.Syslog.LogEveryMessage {
		t.Error("per-message logging must be off by default")
	}

	if runtime.GOOS != "windows" {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if perm := info.Mode().Perm(); perm != 0600 {
			t.Errorf("config permissions = %o, want 600 (may hold DB password / webhook URL)", perm)
		}
	}
}

func TestPartialConfigKeepsRemainingDefaults(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	os.WriteFile(path, []byte(`{"dedup":{"window_seconds":30},"web":{"basic_auth_user":"admin"}}`), 0600)

	cfg, created, err := LoadConfig(path)
	if err != nil || created {
		t.Fatalf("created=%v err=%v", created, err)
	}
	if cfg.Dedup.WindowSeconds != 30 {
		t.Errorf("explicit value lost: %d", cfg.Dedup.WindowSeconds)
	}
	if !cfg.Dedup.Enabled || cfg.Dedup.MaxEntries != 10000 || cfg.Dedup.MaxIntervalSeconds != 60 {
		t.Errorf("omitted dedup fields must keep their defaults: %+v", cfg.Dedup)
	}
	if cfg.Web.BasicAuthUser != "admin" {
		t.Error("web.basic_auth_user not loaded")
	}
}

func TestLoadConfigRejectsBrokenJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	os.WriteFile(path, []byte(`{"dedup": {`), 0600)

	if _, _, err := LoadConfig(path); err == nil {
		t.Error("a syntactically broken config must be an error, not silently replaced by defaults")
	}
}

func TestValidateFixesDangerousValues(t *testing.T) {
	cfg, _, _ := LoadConfig("")
	cfg.RateLimiter.MessagesPerSecond = 0 // ar bloca tot traficul
	cfg.RateLimiter.BurstSize = 0
	cfg.Dedup.WindowSeconds = -5
	cfg.Dedup.MaxIntervalSeconds = 1
	cfg.Dedup.MaxEntries = 0
	cfg.Syslog.UDPQueueSize = 0
	cfg.Syslog.MaxTCPConnections = -1
	cfg.Web.BasicAuthUser = "admin" // parolă lipsă

	warnings := cfg.Validate()

	if cfg.RateLimiter.MessagesPerSecond != 100 || cfg.RateLimiter.BurstSize != 200 {
		t.Errorf("rate limiter not repaired: %+v", cfg.RateLimiter)
	}
	if cfg.Dedup.WindowSeconds != 10 || cfg.Dedup.MaxIntervalSeconds != 60 || cfg.Dedup.MaxEntries != 10000 {
		t.Errorf("dedup not repaired: %+v", cfg.Dedup)
	}
	if cfg.Syslog.UDPQueueSize != 8192 || cfg.Syslog.MaxTCPConnections != 1000 {
		t.Errorf("syslog block not repaired: %+v", cfg.Syslog)
	}
	if len(warnings) < 7 {
		t.Errorf("every repair must be reported, got %d warnings: %v", len(warnings), warnings)
	}
	if !strings.Contains(strings.Join(warnings, "\n"), "basic_auth_password") {
		t.Errorf("empty auth password must be flagged: %v", warnings)
	}
}

func TestValidateLeavesGoodConfigAlone(t *testing.T) {
	cfg, _, _ := LoadConfig("")
	if warnings := cfg.Validate(); len(warnings) != 0 {
		t.Errorf("the default configuration must validate cleanly, got %v", warnings)
	}
}

func TestAllowedOriginsDefaultAndValidation(t *testing.T) {
	cfg, _, _ := LoadConfig("")
	if cfg.Web.AllowedOrigins == nil || len(cfg.Web.AllowedOrigins) != 0 {
		t.Errorf("allowed_origins must default to an empty list, got %v", cfg.Web.AllowedOrigins)
	}

	cfg.Web.AllowedOrigins = []string{"https://rolink.network"}
	if w := cfg.Validate(); len(w) != 0 {
		t.Errorf("a well-formed origin must not warn: %v", w)
	}

	cfg.Web.AllowedOrigins = []string{"rolink.network"}
	if w := strings.Join(cfg.Validate(), "\n"); !strings.Contains(w, "https://host") {
		t.Errorf("an origin without a scheme must be flagged: %q", w)
	}

	cfg.Web.AllowedOrigins = []string{"*"}
	if w := strings.Join(cfg.Validate(), "\n"); !strings.Contains(w, "any website") {
		t.Errorf(`"*" must produce a warning: %q`, w)
	}
}
