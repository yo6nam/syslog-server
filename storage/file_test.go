package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"syslog-server/types"
)

// writeDay scrie n mesaje în fișierul JSON (+ .log) al zilei date, la ore distanțate.
func writeDay(t testing.TB, dir string, day time.Time, n int, content func(i int) string) {
	t.Helper()
	name := day.Format("2006-01-02")

	jf, err := os.Create(filepath.Join(dir, "syslog-"+name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	defer jf.Close()
	enc := json.NewEncoder(jf)

	for i := 0; i < n; i++ {
		ts := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, time.UTC).Add(time.Duration(i) * (23 * time.Hour / time.Duration(n+1)))
		enc.Encode(map[string]interface{}{
			"timestamp": ts, "hostname": fmt.Sprintf("host-%d", i%5), "tag": "t",
			"content": content(i), "severity": 6, "facility": 1,
			"remote_ip": "10.0.0.1", "raw_message": "raw",
		})
	}
	os.WriteFile(filepath.Join(dir, "syslog-"+name+".log"), []byte("text\n"), 0644)
}

func plain(i int) string { return fmt.Sprintf("message %d", i) }

func day(y int, m time.Month, d int) time.Time { return time.Date(y, m, d, 0, 0, 0, 0, time.UTC) }

func newFS(t *testing.T) (*FileStorage, string) {
	t.Helper()
	dir := t.TempDir()
	return nil, dir
}

func TestSearchIsBoundedToTheRequestedDays(t *testing.T) {
	_, dir := newFS(t)
	for d := 10; d <= 19; d++ {
		writeDay(t, dir, day(2026, 8, d), 5, plain)
	}
	writeDay(t, dir, day(2020, 1, 1), 5, plain) // în afara oricărui interval cerut

	fs, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}

	from, to := day(2026, 8, 13), time.Date(2026, 8, 14, 23, 59, 59, 0, time.UTC)
	msgs, total, err := fs.SearchMessages("", "", -1, -1, from, to, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 10 || len(msgs) != 10 {
		t.Fatalf("2 days x 5 messages: total=%d returned=%d, want 10/10", total, len(msgs))
	}
	for _, m := range msgs {
		if m.Timestamp.Before(from) || m.Timestamp.After(to) {
			t.Errorf("message %v is outside the requested range", m.Timestamp)
		}
	}
}

func TestSearchRollingWindowAcrossMidnight(t *testing.T) {
	_, dir := newFS(t)
	writeDay(t, dir, day(2026, 8, 15), 5, plain)
	writeDay(t, dir, day(2026, 8, 16), 5, plain)
	fs, _ := NewFileStorage(dir)

	// "Last 24h": nu se aliniază la miezul nopții și atinge două fișiere-zi.
	from := time.Date(2026, 8, 15, 14, 0, 0, 0, time.UTC)
	to := time.Date(2026, 8, 16, 14, 0, 0, 0, time.UTC)
	msgs, total, err := fs.SearchMessages("", "", -1, -1, from, to, 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range msgs {
		if m.Timestamp.Before(from) || m.Timestamp.After(to) {
			t.Errorf("message %v escaped the rolling window", m.Timestamp)
		}
	}
	if total == 0 || total != len(msgs) {
		t.Errorf("total=%d returned=%d: expected a non-empty window", total, len(msgs))
	}
}

func TestSearchTermHostnameAndPagination(t *testing.T) {
	_, dir := newFS(t)
	writeDay(t, dir, day(2026, 8, 13), 20, func(i int) string {
		if i%4 == 0 {
			return fmt.Sprintf("needle %d", i)
		}
		return plain(i)
	})
	fs, _ := NewFileStorage(dir)
	from, to := day(2026, 8, 13), time.Date(2026, 8, 13, 23, 59, 59, 0, time.UTC)

	_, total, _ := fs.SearchMessages("NEEDLE", "", -1, -1, from, to, 100, 0)
	if total != 5 {
		t.Errorf("case-insensitive term search: total=%d, want 5", total)
	}

	_, total, _ = fs.SearchMessages("", "host-2", -1, -1, from, to, 100, 0)
	if total != 4 {
		t.Errorf("hostname filter: total=%d, want 4", total)
	}

	page1, total, _ := fs.SearchMessages("", "", -1, -1, from, to, 7, 0)
	page2, _, _ := fs.SearchMessages("", "", -1, -1, from, to, 7, 7)
	if len(page1) != 7 || total != 20 || page1[0].Timestamp.Equal(page2[0].Timestamp) {
		t.Errorf("pagination broken: page1=%d total=%d", len(page1), total)
	}
}

// Regresie: fix-ul "Last 24h = 5000" a eșuat o dată pentru că un plafon rămăsese
// la 500 pe drum. Storage-ul trebuie să returneze efectiv până la 5000.
func TestSearchReturnsUpTo5000AndReportsTheRealTotal(t *testing.T) {
	_, dir := newFS(t)
	writeDay(t, dir, day(2026, 8, 13), 6000, plain)
	fs, _ := NewFileStorage(dir)
	from, to := day(2026, 8, 13), time.Date(2026, 8, 13, 23, 59, 59, 0, time.UTC)

	for limit, want := range map[int]int{500: 500, 5000: 5000, 9999: 5000} {
		msgs, total, err := fs.SearchMessages("", "", -1, -1, from, to, limit, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(msgs) != want || total != 6000 {
			t.Errorf("limit %d: returned=%d total=%d, want %d/6000", limit, len(msgs), total, want)
		}
	}
}

func TestDeleteOldMessagesKeepsBoundaryDayAndRemovesLogsToo(t *testing.T) {
	_, dir := newFS(t)
	now := time.Now().UTC()
	for offset := 0; offset < 15; offset++ {
		writeDay(t, dir, now.AddDate(0, 0, -offset), 3, plain)
	}
	fs, _ := NewFileStorage(dir)

	deleted, err := fs.DeleteOldMessages(now.AddDate(0, 0, -7))
	if err != nil {
		t.Fatal(err)
	}
	// Zilele 8..14 (strict mai vechi) dispar; ziua-graniță (7) se păstrează
	// conservator, fișierele fiind pe zi întreagă.
	if deleted != 21 {
		t.Errorf("deleted %d messages, want 21", deleted)
	}

	jsons, _ := filepath.Glob(filepath.Join(dir, "syslog-*.json"))
	logs, _ := filepath.Glob(filepath.Join(dir, "syslog-*.log"))
	if len(jsons) != 8 || len(logs) != 8 {
		t.Errorf("remaining files json=%d log=%d, want 8/8", len(jsons), len(logs))
	}

	total, _ := fs.GetTotalMessageCount()
	if total != 24 {
		t.Errorf("count after purge = %d, want 24 (cache must be invalidated)", total)
	}

	again, _ := fs.DeleteOldMessages(now.AddDate(0, 0, -7))
	if again != 0 {
		t.Errorf("second purge with the same cutoff deleted %d, want 0 (idempotent)", again)
	}
}

func TestDeleteOldMessagesIgnoresUnexpectedFileNames(t *testing.T) {
	_, dir := newFS(t)
	os.WriteFile(filepath.Join(dir, "syslog-notes.json"), []byte("{}\n"), 0644)
	os.WriteFile(filepath.Join(dir, "syslog-backup-2020.json"), []byte("{}\n"), 0644)
	fs, _ := NewFileStorage(dir)

	if _, err := fs.DeleteOldMessages(time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"syslog-notes.json", "syslog-backup-2020.json"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Errorf("%s does not look like a day file and must never be deleted", name)
		}
	}
}

func TestStoreFeedsCacheAndCounts(t *testing.T) {
	_, dir := newFS(t)
	fs, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		err := fs.Store(&types.SyslogMessage{
			Timestamp: time.Now(), Hostname: "h", Tag: "t", Content: fmt.Sprintf("m%d", i),
			Severity: 6, Facility: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	recent, _ := fs.GetRecentMessages(5)
	if len(recent) != 5 || recent[0].Content != "m4" || recent[4].Content != "m0" {
		t.Errorf("recent messages must be newest-first, got %v", recent)
	}
	if total, _ := fs.GetTotalMessageCount(); total != 5 {
		t.Errorf("total = %d, want 5", total)
	}

	page, total, _ := fs.GetMessagesPaginated(0, 3)
	if len(page) != 3 || total != 5 || page[0].Content != "m4" {
		t.Errorf("first page must come from the cache newest-first: %v (total %d)", page, total)
	}
}

func BenchmarkStore(b *testing.B) {
	fs, err := NewFileStorage(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	msg := &types.SyslogMessage{
		Timestamp: time.Now(), Hostname: "yo6nam", Tag: "kernel", Severity: 3, Facility: 0,
		Content:    "xradio TXRX-ERR: [RX] IF=0, Receive failure: 4.",
		RawMessage: "<3>Oct 18 09:53:10 yo6nam kernel: [404590.323487] xradio TXRX-ERR: [RX] IF=0, Receive failure: 4.",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := fs.Store(msg); err != nil {
			b.Fatal(err)
		}
	}
}
