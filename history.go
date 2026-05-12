// pouch-vault history / get — surface the catalog's free
// versioning. Every drop ever mirrored down is a row in the local
// SQLite; multiple drops with the same label or original_path = a
// version chain. Slice 8c of Phase 5.
//
// Subcommands:
//   pouch-vault history [--stream] [--label] [--path] [--limit]
//   pouch-vault get <drop-id> [--extract <out-path>]
package main

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func runHistory(args []string) error {
	fs := flag.NewFlagSet("history", flag.ExitOnError)
	var (
		stream string
		label  string
		path   string
		limit  int
	)
	fs.StringVar(&stream, "stream", "", "filter by exact stream name")
	fs.StringVar(&label, "label", "", "filter by label substring (case-insensitive)")
	fs.StringVar(&path, "path", "", "filter by exact original_path")
	fs.IntVar(&limit, "limit", 50, "max rows returned")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := &config{}
	if err := cfg.finishLoadVaultPaths(); err != nil {
		return err
	}
	if cfg.dbPath == "" {
		if d, err := defaultDBPath(); err == nil {
			cfg.dbPath = d
		}
	}
	if cfg.dbPath == "" {
		return fmt.Errorf("VAULT_DB unset and no default")
	}
	store, err := OpenStore(cfg.dbPath)
	if err != nil {
		return err
	}
	defer store.Close()

	drops, err := store.ListHistory(context.Background(), HistoryFilter{
		Stream: stream,
		Label:  label,
		Path:   path,
		Limit:  limit,
	})
	if err != nil {
		return err
	}
	if len(drops) == 0 {
		fmt.Println("no drops match")
		return nil
	}

	fmt.Printf("%-22s %-19s %10s  %-14s %s\n",
		"DROP_ID", "RECEIVED_AT", "SIZE", "STREAM", "LABEL")
	for _, d := range drops {
		size := d.BodySize
		if size == 0 {
			size = int64(len(d.Body))
		}
		label := d.Label
		if d.OriginalPath != "" && d.OriginalPath != d.Label {
			label += " (" + d.OriginalPath + ")"
		}
		fmt.Printf("%-22s %-19s %10s  %-14s %s\n",
			d.DropID,
			d.ReceivedAt.Local().Format("2026-01-02 15:04:05"),
			humanBytes(size),
			truncate(d.Stream, 14),
			label,
		)
	}
	return nil
}

func runGet(args []string) error {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	var extract string
	fs.StringVar(&extract, "extract", "", "write body bytes to this file path (default: stdout)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() < 1 {
		return fmt.Errorf("usage: pouch-vault get <drop-id> [--extract <path>]")
	}
	dropID := fs.Arg(0)

	cfg := &config{}
	if err := cfg.finishLoadVaultPaths(); err != nil {
		return err
	}
	if cfg.dbPath == "" {
		if d, err := defaultDBPath(); err == nil {
			cfg.dbPath = d
		}
	}
	if cfg.dbPath == "" {
		return fmt.Errorf("VAULT_DB unset and no default")
	}
	store, err := OpenStore(cfg.dbPath)
	if err != nil {
		return err
	}
	defer store.Close()

	d, err := store.Get(context.Background(), dropID)
	if err != nil {
		return err
	}
	if d == nil {
		return fmt.Errorf("drop %s not found", dropID)
	}

	var bytes []byte
	switch d.BodyEncoding {
	case "base64":
		bytes, err = base64.StdEncoding.DecodeString(d.Body)
		if err != nil {
			return fmt.Errorf("decode base64: %w", err)
		}
	case "blob":
		// Body lives on disk at body_blob_path (relative to blobsDir).
		if d.BodyBlobPath == "" {
			return fmt.Errorf("drop %s is a blob but body_blob_path is empty", dropID)
		}
		blobsDir, _ := dataDir()
		if blobsDir != "" {
			blobsDir = filepath.Join(blobsDir, "blobs")
		}
		bytes, err = os.ReadFile(filepath.Join(blobsDir, d.BodyBlobPath))
		if err != nil {
			return fmt.Errorf("read blob: %w", err)
		}
	default: // utf8
		bytes = []byte(d.Body)
	}

	if extract == "" {
		_, err = os.Stdout.Write(bytes)
		return err
	}
	if err := os.MkdirAll(filepath.Dir(extract), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(extract, bytes, 0o644); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "extracted %s (%s) to %s\n",
		dropID, humanBytes(int64(len(bytes))), extract)
	return nil
}

// finishLoadVaultPaths is a slim load — history / get only need
// the DB path, not the full server-side config. Reuses the
// envfile machinery via the existing loadConfig path, but doesn't
// fail when cloud creds are absent (they aren't needed here).
func (c *config) finishLoadVaultPaths() error {
	// Pick from env + envfile via the existing helpers. The full
	// loadConfig() validates POUCH_URL / VAULT_KEY / HMAC; we
	// can't call it here because it returns an error when those
	// are unset.
	cfgFile := os.Getenv("POUCH_VAULT_CONFIG")
	if userCfg, err := configPath(); err == nil && cfgFile == "" {
		// loadEnvFile is no-op on missing files.
		_ = loadEnvFile(userCfg)
	}
	if cfgFile != "" {
		_ = loadEnvFile(cfgFile)
	}
	_ = loadEnvFile("/etc/pouch/vault.env")
	c.dbPath = os.Getenv("VAULT_DB")
	return nil
}

func humanBytes(n int64) string {
	if n < 1024 {
		return fmt.Sprintf("%d B", n)
	}
	if n < 1024*1024 {
		return fmt.Sprintf("%.1f KB", float64(n)/1024)
	}
	return fmt.Sprintf("%.1f MB", float64(n)/(1024*1024))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

// Trim the format-string warning: time.RFC3339-style local time
// is plenty for the table. (Unused symbol guard for older Go
// linters that miss `time` package usage via Format.)
var _ = time.Now