package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
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

// Regresie pentru eșecul din CI ("total = 6, want 5"): un Store() concurent cu
// pornirea storage-ului nu trebuie să fie numărat de două ori sau să apară
// dublat în recentCache. Repetat de multe ori, ca să dea race-ului ocazia să apară.
func TestStoreDuringStartupIsNotDoubleCountedOrDuplicated(t *testing.T) {
	for iteration := 0; iteration < 40; iteration++ {
		dir := t.TempDir()
		writeDay(t, dir, time.Now().UTC().AddDate(0, 0, -1), 100, plain)

		fs, err := NewFileStorage(dir)
		if err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		for w := 0; w < 4; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				for i := 0; i < 25; i++ {
					fs.Store(&types.SyslogMessage{
						Timestamp: time.Now(), Hostname: "h", Tag: "t",
						Content:  fmt.Sprintf("new-%d-%d", w, i),
						Severity: 6, Facility: 1,
					})
				}
			}(w)
		}
		wg.Wait()

		total, err := fs.GetTotalMessageCount()
		if err != nil || total != 200 {
			t.Fatalf("iteration %d: total = %d, want 200 (100 existing + 100 new)", iteration, total)
		}

		recent, _ := fs.GetRecentMessages(200)
		seen := map[string]bool{}
		for _, m := range recent {
			if seen[m.Content] {
				t.Fatalf("iteration %d: %q appears twice in the recent cache", iteration, m.Content)
			}
			seen[m.Content] = true
		}
		if len(recent) != 200 {
			t.Fatalf("iteration %d: recent cache holds %d messages, want 200", iteration, len(recent))
		}
	}
}

func TestCountIsIncrementalAndFollowsTheDisk(t *testing.T) {
	_, dir := newFS(t)
	fs, _ := NewFileStorage(dir)

	count := func() int {
		t.Helper()
		n, err := fs.GetTotalMessageCount()
		if err != nil {
			t.Fatal(err)
		}
		return n
	}

	if count() != 0 {
		t.Fatal("empty directory must count 0")
	}

	path := filepath.Join(dir, "syslog-2026-08-13.json")
	os.WriteFile(path, []byte("{\"a\":1}\n{\"b\":"), 0644) // a doua linie e încă în scriere
	if got := count(); got != 1 {
		t.Errorf("an unfinished line must not be counted yet, got %d", got)
	}

	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	f.WriteString("2}\n{\"c\":3}\n")
	f.Close()
	if got := count(); got != 3 {
		t.Errorf("after the line is finished and one more appended: got %d, want 3", got)
	}

	os.WriteFile(path, []byte("{\"x\":1}\n"), 0644) // trunchiat/rescris
	if got := count(); got != 1 {
		t.Errorf("a shrunk file must be recounted from scratch, got %d", got)
	}

	writeDay(t, dir, day(2026, 8, 14), 7, plain)
	if got := count(); got != 8 {
		t.Errorf("a new day file must be picked up, got %d, want 8", got)
	}

	os.Remove(filepath.Join(dir, "syslog-2026-08-14.json")) // ca la retenție
	if got := count(); got != 1 {
		t.Errorf("a removed file must stop counting, got %d, want 1", got)
	}
}

func TestWarmupLoadsTheNewestMessagesFromTheTail(t *testing.T) {
	old := tailChunkSize
	tailChunkSize = 300 // forțează mai multe dublări ale ferestrei și linii tăiate la margine
	defer func() { tailChunkSize = old }()

	_, dir := newFS(t)
	writeDay(t, dir, day(2026, 8, 12), 450, func(i int) string { return fmt.Sprintf("old-%04d", i) })
	writeDay(t, dir, day(2026, 8, 13), 2000, func(i int) string { return fmt.Sprintf("new-%04d", i) })

	fs, err := NewFileStorage(dir)
	if err != nil {
		t.Fatal(err)
	}

	recent, _ := fs.GetRecentMessages(500)
	if len(recent) != 500 {
		t.Fatalf("recent cache = %d messages, want 500", len(recent))
	}
	if recent[0].Content != "new-1999" || recent[499].Content != "new-1500" {
		t.Errorf("must hold the newest 500 (new-1999 .. new-1500), got %q .. %q", recent[0].Content, recent[499].Content)
	}
	for i := 1; i < len(recent); i++ {
		if recent[i].Timestamp.After(recent[i-1].Timestamp) {
			t.Fatalf("cache not newest-first at position %d", i)
		}
	}
}

func TestWarmupSpansFilesWhenTheNewestDayIsShort(t *testing.T) {
	_, dir := newFS(t)
	writeDay(t, dir, day(2026, 8, 12), 450, func(i int) string { return fmt.Sprintf("old-%04d", i) })
	writeDay(t, dir, day(2026, 8, 13), 50, func(i int) string { return fmt.Sprintf("new-%04d", i) })

	fs, _ := NewFileStorage(dir)
	recent, _ := fs.GetRecentMessages(500)

	if len(recent) != 500 || recent[0].Content != "new-0049" || recent[49].Content != "new-0000" || recent[50].Content != "old-0449" {
		t.Fatalf("must continue into the previous day: len=%d first=%q [49]=%q [50]=%q",
			len(recent), recent[0].Content, recent[49].Content, recent[50].Content)
	}
}

func TestWarmupIgnoresCorruptOrUnfinishedLines(t *testing.T) {
	_, dir := newFS(t)
	good := `{"timestamp":"2026-08-13T10:00:00Z","hostname":"h","content":"ok","severity":6,"facility":1}`
	os.WriteFile(filepath.Join(dir, "syslog-2026-08-13.json"),
		[]byte(good+"\nnot json at all\n"+good+"\n{\"timestamp\":\"2026-08-13T11:00"), 0644)

	fs, _ := NewFileStorage(dir)
	recent, _ := fs.GetRecentMessages(10)
	if len(recent) != 2 {
		t.Errorf("only the 2 valid lines must load, got %d", len(recent))
	}
}

func TestCountOnlyCountsJSONDayFiles(t *testing.T) {
	_, dir := newFS(t)
	writeDay(t, dir, day(2026, 8, 13), 4, plain) // scrie și .log-ul zilei
	fs, _ := NewFileStorage(dir)

	if got, _ := fs.GetTotalMessageCount(); got != 4 {
		t.Errorf("count = %d, want 4 (the .log text file must not be counted)", got)
	}
}

// ---------------------------------------------------------------------------
// Fișiere-zi corupte. Un restart sau o cădere de curent poate lăsa o linie
// neterminată sau blocuri de NUL la finalul fișierului de azi. Vechiul cod
// (json.Decoder cu `continue` pe eroare) intra atunci într-o buclă infinită, cu
// un nucleu la 100% CPU, pe ORICARE dintre căile de citire.
// ---------------------------------------------------------------------------

const validJSONLine = `{"timestamp":"2026-08-13T10:00:00Z","hostname":"h","tag":"t","content":"ok","severity":6,"facility":1,"remote_ip":"10.0.0.1","raw_message":"raw"}`

// mustFinish pică repede (în loc să blocheze toată suita) dacă fn nu se termină.
func mustFinish(t *testing.T, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not return within 5s: infinite loop on a corrupt line?", what)
	}
}

func TestCorruptDayFilesNeverHangAndKeepTheValidMessages(t *testing.T) {
	cases := []struct {
		name    string
		content string
		want    int // mesaje valide care trebuie recuperate
	}{
		{"truncated last line", validJSONLine + "\n" + validJSONLine + "\n" + `{"timestamp":"2026-08-13T11:00`, 2},
		{"NUL block between lines", validJSONLine + "\n\x00\x00\x00\x00\n" + validJSONLine + "\n", 2},
		{"NUL tail without newline", validJSONLine + "\n" + validJSONLine + "\n" + strings.Repeat("\x00", 4096), 2},
		{"garbage line in the middle", validJSONLine + "\nnot json at all\n" + validJSONLine + "\n", 2},
		{"only garbage", "garbage\n{{{{\n]]]\n", 0},
		{"empty file", "", 0},
		{"CRLF and blank lines", validJSONLine + "\r\n\r\n" + validJSONLine + "\r\n", 2},
		{"unterminated last valid line", validJSONLine + "\n" + validJSONLine, 2},
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			os.WriteFile(filepath.Join(dir, "syslog-2026-08-13.json"), []byte(c.content), 0644)
			fs, err := NewFileStorage(dir)
			if err != nil {
				t.Fatal(err)
			}

			from, to := day(2026, 8, 13), time.Date(2026, 8, 13, 23, 59, 59, 0, time.UTC)

			mustFinish(t, "GetMessagesByDate", func() {
				msgs, err := fs.GetMessagesByDate(from)
				if err != nil || len(msgs) != c.want {
					t.Errorf("GetMessagesByDate: %d messages (err %v), want %d", len(msgs), err, c.want)
				}
			})
			mustFinish(t, "SearchMessages", func() {
				_, total, err := fs.SearchMessages("", "", -1, -1, from, to, 100, 0)
				if err != nil || total != c.want {
					t.Errorf("SearchMessages: total %d (err %v), want %d", total, err, c.want)
				}
			})
			mustFinish(t, "GetRecentMessages (disk fallback)", func() {
				// limit > cache: forțează citirea completă de pe disc
				if msgs, err := fs.GetRecentMessages(600); err != nil || len(msgs) != c.want {
					t.Errorf("GetRecentMessages: %d messages (err %v), want %d", len(msgs), err, c.want)
				}
			})
			mustFinish(t, "GetMessagesPaginated (disk fallback)", func() {
				if _, _, err := fs.GetMessagesPaginated(1, 10); err != nil {
					t.Errorf("GetMessagesPaginated: %v", err)
				}
			})
			mustFinish(t, "GetTotalMessageCount", func() {
				if _, err := fs.GetTotalMessageCount(); err != nil {
					t.Errorf("GetTotalMessageCount: %v", err)
				}
			})
			mustFinish(t, "DeleteOldMessages", func() {
				if _, err := fs.DeleteOldMessages(time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)); err != nil {
					t.Errorf("DeleteOldMessages: %v", err)
				}
			})
		})
	}
}

// Un mesaj foarte lung (raw_message de sute de KB) trebuie citit întreg; o
// "optimizare" cu bufio.Scanner (limită de 64 KB/linie) l-ar pierde în tăcere.
func TestVeryLongValidLineIsRead(t *testing.T) {
	dir := t.TempDir()
	long := fmt.Sprintf(`{"timestamp":"2026-08-13T10:00:00Z","hostname":"h","content":"big","severity":6,"facility":1,"raw_message":%q}`,
		strings.Repeat("x", 300*1024))
	os.WriteFile(filepath.Join(dir, "syslog-2026-08-13.json"), []byte(validJSONLine+"\n"+long+"\n"+validJSONLine+"\n"), 0644)
	fs, _ := NewFileStorage(dir)

	msgs, err := fs.GetMessagesByDate(day(2026, 8, 13))
	if err != nil || len(msgs) != 3 {
		t.Fatalf("got %d messages (err %v), want 3 including the 300 KB one", len(msgs), err)
	}
	if len(msgs[1].RawMessage) != 300*1024 {
		t.Errorf("long message truncated: %d bytes", len(msgs[1].RawMessage))
	}
}
