// Snapshot pipeline tests — slice 8f.
//
// Two layers covered:
//   - parseSnapshotInterval (pure)
//   - produceSqliteSnapshot (integration with the sqlite3 CLI)
//
// The integration test creates a real on-disk sqlite database with
// a few rows, runs the backup, and verifies the artifact is itself
// a valid sqlite file containing the same rows.

package main

import (
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// driver name for sql.Open — pouch-vault uses the pure-Go modernc.org
// driver registered under "sqlite".
const sqliteDriverName = "sqlite"

func TestParseSnapshotInterval(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
		bad  bool
	}{
		{"", 0, false},
		{"on-demand", 0, false},
		{"5m", 5 * time.Minute, false},
		{"1h", time.Hour, false},
		{"6h", 6 * time.Hour, false},
		{"15m", 15 * time.Minute, false},
		{"1d", 24 * time.Hour, false},
		{"3d", 72 * time.Hour, false},
		{"0s", 0, true},     // zero-or-negative refused
		{"-1m", 0, true},    // negative refused
		{"hello", 0, true},  // unparseable
		{"1d.5", 0, true},   // bad day form
	}
	for _, c := range cases {
		got, err := parseSnapshotInterval(c.in)
		if c.bad {
			if err == nil {
				t.Errorf("parseSnapshotInterval(%q) = %v, want error", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseSnapshotInterval(%q): %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("parseSnapshotInterval(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestProduceSqliteSnapshot(t *testing.T) {
	// Skip if sqlite3 CLI isn't on PATH — the test depends on a
	// well-known shell-out and shouldn't fail CI on a stripped image.
	if _, err := exec.LookPath("sqlite3"); err != nil {
		t.Skip("sqlite3 CLI not on PATH; skipping integration test")
	}

	tmp := t.TempDir()
	srcPath := filepath.Join(tmp, "src.db")
	outDir := filepath.Join(tmp, "snapshots")

	// 1. Seed a source DB with a few rows.
	src, err := sql.Open(sqliteDriverName, srcPath)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	if _, err := src.Exec(`CREATE TABLE t(id INTEGER PRIMARY KEY, v TEXT)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 1; i <= 5; i++ {
		if _, err := src.Exec(`INSERT INTO t (v) VALUES (?)`, "row-"+string(rune('A'+i-1))); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	src.Close()

	// 2. Take a snapshot.
	artifact, err := produceSqliteSnapshot(srcPath, outDir)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	if _, err := os.Stat(artifact); err != nil {
		t.Fatalf("artifact missing: %v", err)
	}
	if artifact != filepath.Join(outDir, "src.db.bak") {
		t.Errorf("artifact path = %q, want .../src.db.bak", artifact)
	}

	// 3. Verify the artifact is a real sqlite database with the same
	//    rows. This is the actual contract — anything that copies
	//    bytes can produce a file; only the online backup API
	//    produces a CONSISTENT one.
	dst, err := sql.Open(sqliteDriverName, artifact)
	if err != nil {
		t.Fatalf("open artifact: %v", err)
	}
	defer dst.Close()
	var n int
	if err := dst.QueryRow(`SELECT COUNT(*) FROM t`).Scan(&n); err != nil {
		t.Fatalf("query artifact: %v", err)
	}
	if n != 5 {
		t.Errorf("artifact has %d rows; want 5", n)
	}

	// 4. Idempotency: running the snapshot again should overwrite,
	//    not error.
	artifact2, err := produceSqliteSnapshot(srcPath, outDir)
	if err != nil {
		t.Fatalf("second produce: %v", err)
	}
	if artifact2 != artifact {
		t.Errorf("second produce returned %q; want same path %q", artifact2, artifact)
	}
}

func TestProduceSqliteSnapshot_RefusesNonRegularFile(t *testing.T) {
	tmp := t.TempDir()
	dirPath := filepath.Join(tmp, "src-dir")
	if err := os.Mkdir(dirPath, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := produceSqliteSnapshot(dirPath, tmp); err == nil {
		t.Error("expected error when source is a directory; got nil")
	}
}

func TestLabelFromTemplate(t *testing.T) {
	if got := labelFromTemplate("", "foo.bak"); got != "foo.bak" {
		t.Errorf("empty template: got %q, want foo.bak", got)
	}
	got := labelFromTemplate("daily backup @ {date}", "x")
	want := "daily backup @ " + time.Now().UTC().Format("2006-01-02")
	if got != want {
		t.Errorf("label = %q, want %q", got, want)
	}
}
