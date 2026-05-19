// Periodic snapshot pipeline — phase 5 slice 8f (schedule + sqlite kind).
//
// For each VAULT_PATHS entry with a `snapshot` block, a per-path
// goroutine ticks at the configured interval and runs the kind-
// specific producer. The producer writes an atomic artifact under
// <vault-data>/snapshots/<safeName>/, which is then handed to the
// existing upload path as a regular drop on the configured stream.
//
// This slice ships kind=sqlite (online .backup API). zip / tar /
// command kinds are part of the slice plan in the accepted decision
// periodic-snapshot-pipeline-for-live-files but not in this commit.
//
// Triggers: the scheduler goroutine (interval string) OR the manual
// CLI subcommand `pouch-vault snapshot <stream>`.

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// SnapshotConfig is the optional `snapshot` block in a VAULT_PATHS
// entry. Presence promotes the entry from fsnotify-driven to
// schedule-driven; the path's `direction` field is ignored when
// snapshot is set.
type SnapshotConfig struct {
	Kind     string `json:"kind"`               // "sqlite" (more kinds in later slices)
	Schedule string `json:"schedule,omitempty"` // "1h", "5m", "15m", "1d", "" or "on-demand"
	Retain   string `json:"retain,omitempty"`   // "latest" (default) | "timestamped"
	Label    string `json:"label_template,omitempty"`
}

const (
	SnapshotKindSqlite        = "sqlite"
	SnapshotRetainLatest      = "latest"
	SnapshotRetainTimestamped = "timestamped"
)

// parseSnapshotInterval converts a schedule string into a Duration.
// "" or "on-demand" returns (0, nil) — caller treats 0 as "skip the
// scheduler, only fire on explicit command".
func parseSnapshotInterval(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" || s == "on-demand" {
		return 0, nil
	}
	// time.ParseDuration handles "1h", "5m", "30s", etc. but not "1d".
	if strings.HasSuffix(s, "d") {
		n, err := strconv.Atoi(strings.TrimSuffix(s, "d"))
		if err != nil {
			return 0, fmt.Errorf("invalid days in %q: %w", s, err)
		}
		if n <= 0 {
			return 0, fmt.Errorf("invalid days in %q: must be > 0", s)
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid schedule %q: %w", s, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("invalid schedule %q: must be > 0", s)
	}
	return d, nil
}

// produceSqliteSnapshot copies srcPath into outDir/<basename>.bak
// using the SQLite online backup API (via the sqlite3 CLI's .backup
// command). Returns the artifact path. The source file is NOT held
// under an exclusive lock — readers and writers continue concurrently.
//
// Refuses non-regular sources (no symlinks, no devices, etc.) so a
// misconfigured VAULT_PATHS entry can't snapshot something dangerous.
func produceSqliteSnapshot(srcPath, outDir string) (string, error) {
	info, err := os.Stat(srcPath)
	if err != nil {
		return "", fmt.Errorf("source %s: %w", srcPath, err)
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("source %s is not a regular file", srcPath)
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return "", err
	}
	base := filepath.Base(srcPath) + ".bak"
	outPath := filepath.Join(outDir, base)
	tmpPath := outPath + ".tmp"
	// Clean any stale tmp from a previous crashed run.
	_ = os.Remove(tmpPath)
	// sqlite3 CLI's `.backup` invokes the online backup API. WAL is
	// implicitly checkpointed into the .bak; sidecar -wal/-shm files
	// are NOT copied (the .bak is self-contained).
	cmd := exec.Command("sqlite3", srcPath, fmt.Sprintf(".backup '%s'", tmpPath))
	if out, err := cmd.CombinedOutput(); err != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("sqlite3 .backup failed: %w: %s", err, strings.TrimSpace(string(out)))
	}
	if err := os.Rename(tmpPath, outPath); err != nil {
		return "", fmt.Errorf("rename %s: %w", outPath, err)
	}
	return outPath, nil
}

// snapshotOutDir returns the per-path directory snapshots land in.
// Path components are slash-replaced so the directory name is safe.
func snapshotOutDir(p ConfigPath) (string, error) {
	d, err := dataDir()
	if err != nil {
		return "", err
	}
	safe := strings.ReplaceAll(strings.TrimPrefix(p.Path, "/"), "/", "_")
	safe = strings.ReplaceAll(safe, "~", "home")
	if safe == "" {
		safe = "root"
	}
	return filepath.Join(d, "snapshots", safe), nil
}

// labelFromTemplate expands {ts} / {date} placeholders. Empty
// template falls back to the artifact basename.
func labelFromTemplate(tpl, base string) string {
	if tpl == "" {
		return base
	}
	now := time.Now().UTC()
	tpl = strings.ReplaceAll(tpl, "{ts}", now.Format("2006-01-02T15:04Z"))
	tpl = strings.ReplaceAll(tpl, "{date}", now.Format("2006-01-02"))
	return tpl
}

// runSnapshotOnce dispatches to the kind-specific producer, then
// uploads the produced artifact as a drop. One artifact per tick.
func runSnapshotOnce(ctx context.Context, client *PouchClient, cfg *config, p ConfigPath, opts syncOpts) error {
	sc := p.Snapshot
	if sc == nil {
		return fmt.Errorf("path %s has no snapshot config", p.Path)
	}
	switch sc.Kind {
	case SnapshotKindSqlite:
		return runSqliteSnapshot(ctx, client, cfg, p, opts)
	default:
		return fmt.Errorf("unknown snapshot kind %q (slice 8f only ships %q)",
			sc.Kind, SnapshotKindSqlite)
	}
}

func runSqliteSnapshot(ctx context.Context, client *PouchClient, _ *config, p ConfigPath, opts syncOpts) error {
	outDir, err := snapshotOutDir(p)
	if err != nil {
		return err
	}
	artifact, err := produceSqliteSnapshot(expandHome(p.Path), outDir)
	if err != nil {
		return fmt.Errorf("produce: %w", err)
	}
	defer func() {
		// Bytes live in the cloud catalog (and replica vaults) after a
		// successful upload — local tmp is disposable. On error the
		// artifact stays so the operator can inspect.
		_ = os.Remove(artifact)
	}()

	base := filepath.Base(artifact)
	origPath := base
	if p.Snapshot.Retain == SnapshotRetainTimestamped {
		ts := time.Now().UTC().Format("2006-01-02T15-04-05")
		origPath = filepath.Join("snapshots", ts+"-"+base)
	}

	info, err := os.Stat(artifact)
	if err != nil {
		return err
	}
	mime := "application/x-sqlite3"
	label := labelFromTemplate(p.Snapshot.Label, base)

	in := DropInput{
		Label:        label,
		MIME:         mime,
		Stream:       p.Stream,
		OriginalPath: origPath,
	}
	if info.Size() > opts.maxInline {
		if opts.uploader == nil {
			return fmt.Errorf("snapshot %d bytes exceeds inline cap and no uploader configured", info.Size())
		}
		shaHex, err := FileSHA(artifact)
		if err != nil {
			return fmt.Errorf("sha256: %w", err)
		}
		blobID, err := opts.uploader.UploadFile(ctx, artifact, mime, shaHex, info.Size())
		if err != nil {
			return fmt.Errorf("chunked upload: %w", err)
		}
		in.BodyBlobID = blobID
		in.BodyEncoding = "blob"
	} else {
		body, err := os.ReadFile(artifact)
		if err != nil {
			return err
		}
		in.BodyEncoding, in.Body = encodeBody(body)
	}
	postCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	out, err := client.PostDrop(postCtx, in)
	if err != nil {
		return fmt.Errorf("post drop: %w", err)
	}
	log.Printf("snapshot: %s -> %s (%d bytes, stream=%s)", base, out.ID, info.Size(), p.Stream)
	return nil
}

// runSnapshotLoop is a per-path goroutine that ticks at the snapshot's
// configured interval. First tick after a ~5% jitter so multiple
// hosts producing the same backup don't pile up at identical wall-
// clock moments. Three consecutive failures log loudly; no alerting
// is wired yet (deferred per the decision body).
func runSnapshotLoop(ctx context.Context, client *PouchClient, cfg *config, p ConfigPath, opts syncOpts) {
	interval, err := parseSnapshotInterval(p.Snapshot.Schedule)
	if err != nil {
		log.Printf("snapshot: %s: %v — disabled", p.Path, err)
		return
	}
	if interval == 0 {
		log.Printf("snapshot: %s: on-demand only (manual `pouch-vault snapshot %s`)",
			p.Path, p.Stream)
		return
	}
	jitter := time.Duration(rand.Int63n(int64(interval/20 + 1)))
	first := interval + jitter
	log.Printf("snapshot: %s every %s (kind=%s, first in %s)",
		p.Path, interval, p.Snapshot.Kind, first.Round(time.Second))
	timer := time.NewTimer(first)
	defer timer.Stop()

	failures := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if err := runSnapshotOnce(ctx, client, cfg, p, opts); err != nil {
				failures++
				log.Printf("snapshot: %s: %v", p.Path, err)
				if failures == 3 {
					log.Printf("snapshot: %s has failed 3 times — check the logs", p.Path)
				}
			} else {
				failures = 0
			}
			timer.Reset(interval)
		}
	}
}

// startSnapshotLoops launches one goroutine per path that has a
// snapshot block. Called from the daemon entry point alongside the
// SSE consumer + watch-producer loop. Returns immediately; goroutines
// run until ctx is cancelled.
func startSnapshotLoops(ctx context.Context, client *PouchClient, cfg *config, opts syncOpts) {
	for _, p := range cfg.paths {
		if p.Snapshot == nil {
			continue
		}
		go runSnapshotLoop(ctx, client, cfg, p, opts)
	}
}

// runSnapshotCommand is the manual CLI entry — runs one snapshot tick
// for the path bound to the given stream. Useful for testing without
// waiting for the scheduler.
func runSnapshotCommand(args []string) error {
	fs := flag.NewFlagSet("snapshot", flag.ExitOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return fmt.Errorf("usage: pouch-vault snapshot <stream>")
	}
	streamName := rest[0]

	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	var target *ConfigPath
	for i := range cfg.paths {
		if cfg.paths[i].Stream == streamName && cfg.paths[i].Snapshot != nil {
			target = &cfg.paths[i]
			break
		}
	}
	if target == nil {
		return fmt.Errorf("no VAULT_PATHS entry binds stream %q with a snapshot block", streamName)
	}
	client := NewPouchClient(cfg.pouchURL, cfg.vaultKey)
	uploader := newUploaderFromConfig(client, cfg)
	opts := syncOpts{maxInline: defaultMaxInline, uploader: uploader}
	return runSnapshotOnce(context.Background(), client, cfg, *target, opts)
}
