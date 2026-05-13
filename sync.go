// pouch-vault sync — one-shot folder-watcher producer.
//
// For each VAULT_PATHS entry with direction='watch', walks the
// folder, computes sha256 per file, compares to the local state
// file, and POSTs new/changed files as drops to pouch via
// X-Vault-Key auth on /api/items.
//
// Schedule via cron / launchd / systemd timer. The long-running
// daemon variant (`pouch-vault watch`) reuses the per-file flow
// from this file.
//
// Per decision vault-producer-mode-and-local-only-git.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"
)

// syncOpts is shared by the sync subcommand CLI and (in slice 8b)
// the watch daemon's per-event handler — both end up calling
// syncPath with these knobs.
type syncOpts struct {
	dry         bool   // log what would happen; skip POSTs + state write
	maxInline   int64  // bytes; files above this use chunked upload
	verbose     bool
	uploader    *Uploader // chunked uploader for files > maxInline
}

const defaultMaxInline = 1 << 20 // 1 MiB — matches existing /api/items cap

func runSync(args []string) error {
	fs := flag.NewFlagSet("sync", flag.ExitOnError)
	var (
		dry     bool
		verbose bool
	)
	fs.BoolVar(&dry, "dry", false, "list what would be dropped; don't POST or update state")
	fs.BoolVar(&verbose, "verbose", false, "log every file scanned (default: only drops + skips)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	if len(cfg.paths) == 0 {
		return fmt.Errorf("VAULT_PATHS empty — nothing to scan")
	}

	state, err := loadSyncState()
	if err != nil {
		return err
	}

	client := NewPouchClient(cfg.pouchURL, cfg.vaultKey)
	uploader := newUploaderFromConfig(client, cfg)
	opts := syncOpts{dry: dry, maxInline: defaultMaxInline, verbose: verbose, uploader: uploader}

	totalDropped, totalSkipped, totalErrors := 0, 0, 0
	for _, p := range cfg.paths {
		if p.effectiveDirection() != "watch" {
			continue
		}
		n, errs := syncPath(context.Background(), client, state, p, opts)
		totalDropped += n
		totalErrors += errs
		if errs > 0 {
			totalSkipped += errs
		}
	}

	if !dry {
		if err := saveSyncState(state); err != nil {
			return fmt.Errorf("save state: %w", err)
		}
	}
	log.Printf("sync: %d dropped, %d errors%s", totalDropped, totalErrors,
		func() string {
			if dry {
				return " (dry run; state not written)"
			}
			return ""
		}())
	if totalErrors > 0 {
		// Non-zero exit so cron / launchd notice. State file still
		// reflects everything that DID succeed.
		return fmt.Errorf("%d file(s) failed; see log", totalErrors)
	}
	return nil
}

// syncPath walks one watch-path, drops new/changed files, returns
// (dropped count, error count).
func syncPath(ctx context.Context, client *PouchClient, state *syncState, p ConfigPath, opts syncOpts) (int, int) {
	root := expandHome(p.Path)
	st, err := os.Stat(root)
	if err != nil {
		log.Printf("sync: skip %s — %v", p.Path, err)
		return 0, 1
	}
	if !st.IsDir() {
		log.Printf("sync: skip %s — not a directory", p.Path)
		return 0, 1
	}

	dropped, errs := 0, 0
	walkErr := filepath.WalkDir(root, func(abs string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		// Skip hidden files / dirs (basename starts with '.').
		if strings.HasPrefix(d.Name(), ".") {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, abs)
		if err != nil {
			return err
		}
		// Symlink loops: follow at most one level (the dir-entry
		// already-resolved). WalkDir doesn't follow symlinks by
		// default — safe.
		n, err := syncFile(ctx, client, state, p, root, rel, abs, opts)
		if err != nil {
			log.Printf("sync: %s: %v", rel, err)
			errs++
			return nil
		}
		dropped += n
		return nil
	})
	if walkErr != nil {
		log.Printf("sync: walk %s: %v", root, walkErr)
		errs++
	}
	if state.Paths[p.Path] != nil {
		state.Paths[p.Path].ScannedAt = time.Now()
	}
	return dropped, errs
}

// syncFile decides whether a single file needs a drop, and POSTs
// when it does. Returns (1, nil) on drop, (0, nil) on skip,
// (0, err) on error.
func syncFile(ctx context.Context, client *PouchClient, state *syncState, p ConfigPath, root, rel, abs string, opts syncOpts) (int, error) {
	info, err := os.Stat(abs)
	if err != nil {
		return 0, err
	}
	// Quick-skip via mtime match — saves the sha256 cost when the
	// file genuinely hasn't changed.
	prev := state.get(p.Path, rel)
	if prev != nil && prev.MTime.Equal(info.ModTime()) && prev.Size == info.Size() {
		if opts.verbose {
			log.Printf("sync: skip %s — mtime+size unchanged", rel)
		}
		return 0, nil
	}

	// Two paths from here:
	//   - small  (size <= maxInline): read fully, base64/utf8, inline POST
	//   - large  (size  > maxInline): chunked-upload via Uploader, POST
	//                                 /api/items with body_blob_id
	if info.Size() > opts.maxInline {
		return syncFileLarge(ctx, client, state, p, rel, abs, info, opts)
	}

	body, err := os.ReadFile(abs)
	if err != nil {
		return 0, err
	}
	sum := sha256.Sum256(body)
	shaHex := hex.EncodeToString(sum[:])

	if prev != nil && prev.SHA256 == shaHex {
		// Mtime moved but content unchanged (e.g. touch). Refresh
		// state's mtime so the cheap-skip works next run; no drop.
		prev.MTime = info.ModTime()
		state.set(p.Path, rel, prev)
		if opts.verbose {
			log.Printf("sync: skip %s — content unchanged (mtime touch)", rel)
		}
		return 0, nil
	}

	encoding, encoded := encodeBody(body)
	in := DropInput{
		Label:        filepath.Base(rel),
		Body:         encoded,
		BodyEncoding: encoding,
		MIME:         mimeFromExt(filepath.Ext(rel)),
		Stream:       p.Stream,
		OriginalPath: rel,
	}
	if opts.dry {
		log.Printf("sync: would drop %s -> stream %s (%d bytes, sha=%s)", rel, p.Stream, info.Size(), shaHex[:8])
		return 1, nil
	}
	postCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	out, err := client.PostDrop(postCtx, in)
	if err != nil {
		return 0, err
	}
	state.set(p.Path, rel, &syncStateFile{
		SHA256: shaHex,
		Size:   info.Size(),
		MTime:  info.ModTime(),
		DropID: out.ID,
		Stream: p.Stream,
	})
	log.Printf("sync: dropped %s -> %s (%d bytes)", rel, out.ID, info.Size())
	return 1, nil
}

// syncFileLarge handles files over the inline cap via the chunked
// upload protocol (Phase 5 slice 8e.producer). Steps:
//   1. Compute sha256 streaming (don't read the whole file into RAM).
//   2. Skip if the sync state already has the file at this sha.
//   3. Hand to the uploader; it resumes-or-opens, chunks, completes.
//   4. POST /api/items with body_blob_id.
//   5. Record sync state with the returned drop_id.
func syncFileLarge(ctx context.Context, client *PouchClient, state *syncState, p ConfigPath, rel, abs string, info os.FileInfo, opts syncOpts) (int, error) {
	if opts.uploader == nil {
		log.Printf("sync: skip %s — %d bytes exceeds inline cap and no uploader configured", rel, info.Size())
		return 0, nil
	}
	shaHex, err := FileSHA(abs)
	if err != nil {
		return 0, fmt.Errorf("sha256: %w", err)
	}
	prev := state.get(p.Path, rel)
	if prev != nil && prev.SHA256 == shaHex {
		prev.MTime = info.ModTime()
		state.set(p.Path, rel, prev)
		if opts.verbose {
			log.Printf("sync: skip %s — large file content unchanged", rel)
		}
		return 0, nil
	}
	if opts.dry {
		log.Printf("sync: would chunk-upload %s -> stream %s (%d bytes, sha=%s)", rel, p.Stream, info.Size(), shaHex[:8])
		return 1, nil
	}

	mime := mimeFromExt(filepath.Ext(rel))
	log.Printf("sync: chunk-uploading %s (%d bytes, sha=%s)", rel, info.Size(), shaHex[:8])
	blobID, err := opts.uploader.UploadFile(ctx, abs, mime, shaHex, info.Size())
	if err != nil {
		return 0, fmt.Errorf("chunked upload: %w", err)
	}

	postCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	out, err := client.PostDrop(postCtx, DropInput{
		Label:        filepath.Base(rel),
		BodyBlobID:   blobID,
		BodyEncoding: "blob",
		MIME:         mime,
		Stream:       p.Stream,
		OriginalPath: rel,
	})
	if err != nil {
		return 0, fmt.Errorf("post drop: %w", err)
	}
	state.set(p.Path, rel, &syncStateFile{
		SHA256: shaHex,
		Size:   info.Size(),
		MTime:  info.ModTime(),
		DropID: out.ID,
		Stream: p.Stream,
	})
	log.Printf("sync: chunk-uploaded %s -> %s (blob %s, %d bytes)",
		rel, out.ID, blobID, info.Size())
	return 1, nil
}

// encodeBody returns (encoding, encoded). UTF-8 text rides as-is;
// anything else (binary bytes, mixed encodings) goes base64. The
// cloud-side body_encoding field decides how the body is read.
func encodeBody(b []byte) (string, string) {
	if utf8.Valid(b) {
		return "utf8", string(b)
	}
	return "base64", base64.StdEncoding.EncodeToString(b)
}

// mimeFromExt is a tiny static lookup. Not exhaustive — covers the
// common scrape outputs. Cloud + receivers can detect from bytes
// later if they care.
func mimeFromExt(ext string) string {
	switch strings.ToLower(ext) {
	case ".json":
		return "application/json"
	case ".html", ".htm":
		return "text/html"
	case ".md":
		return "text/markdown"
	case ".txt":
		return "text/plain"
	case ".csv":
		return "text/csv"
	case ".xml":
		return "application/xml"
	case ".yaml", ".yml":
		return "application/yaml"
	case ".png":
		return "image/png"
	case ".jpg", ".jpeg":
		return "image/jpeg"
	case ".pdf":
		return "application/pdf"
	}
	return ""
}

// expandHome resolves a leading ~ to the user's home directory.
// No-op when the path doesn't start with ~ — keeps the parser
// trivially idempotent.
func expandHome(p string) string {
	if !strings.HasPrefix(p, "~") {
		return p
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return p
	}
	if p == "~" {
		return home
	}
	if strings.HasPrefix(p, "~/") {
		return filepath.Join(home, p[2:])
	}
	return p
}
