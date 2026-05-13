// pouch-vault watch — long-running fsnotify daemon variant of sync.
//
// Same per-file flow as sync (syncPath / syncFile from sync.go),
// but the trigger is filesystem events instead of a scheduled scan.
// 500ms debounce coalesces rapid-fire events (editor save-then-
// rename, build tools that touch many files). A 30s safety-scan
// ticker re-runs syncPath on every watched root regardless of
// events — catches fsnotify event drops under load and new
// subdirectories that haven't been Add()ed to the watcher yet.
//
// Per decision vault-producer-mode-and-local-only-git +
// task pouch-vault-watch.
package main

import (
	"context"
	"errors"
	"flag"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
)

const (
	watchDebounce   = 500 * time.Millisecond
	watchSafetyScan = 30 * time.Second
)

func runWatch(args []string) error {
	fs := flag.NewFlagSet("watch", flag.ExitOnError)
	var verbose bool
	fs.BoolVar(&verbose, "verbose", false, "log every file scanned")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	watchPaths := []ConfigPath{}
	for _, p := range cfg.paths {
		if p.effectiveDirection() == "watch" {
			watchPaths = append(watchPaths, p)
		}
	}
	if len(watchPaths) == 0 {
		return errors.New("VAULT_PATHS has no direction='watch' entries — nothing to do")
	}

	state, err := loadSyncState()
	if err != nil {
		return err
	}
	client := NewPouchClient(cfg.pouchURL, cfg.vaultKey)
	uploader := newUploaderFromConfig(client, cfg)
	opts := syncOpts{maxInline: defaultMaxInline, verbose: verbose, uploader: uploader}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	// Initial subscribe + boot scan. The boot scan applies the
	// same per-file logic so files written before the watcher was
	// running get picked up immediately, not on the next 30s tick.
	rootByPath := map[string]ConfigPath{}
	for _, p := range watchPaths {
		root := expandHome(p.Path)
		if err := addRecursive(watcher, root); err != nil {
			log.Printf("watch: subscribe %s: %v", root, err)
			continue
		}
		rootByPath[root] = p
		dropped, _ := syncPath(context.Background(), client, state, p, opts)
		log.Printf("watch: boot scan %s -> %d dropped", root, dropped)
	}
	if err := saveSyncState(state); err != nil {
		log.Printf("watch: save state after boot: %v", err)
	}

	// Per-path debounce timers. fsnotify events on any file under
	// root reset that root's timer; when no events have fired for
	// watchDebounce, we run syncPath for that root.
	mu := sync.Mutex{}
	dirty := map[string]bool{}
	timers := map[string]*time.Timer{}

	fire := func(root string, p ConfigPath) {
		mu.Lock()
		delete(timers, root)
		if !dirty[root] {
			mu.Unlock()
			return
		}
		dirty[root] = false
		mu.Unlock()
		dropped, errs := syncPath(context.Background(), client, state, p, opts)
		if dropped > 0 || errs > 0 {
			if err := saveSyncState(state); err != nil {
				log.Printf("watch: save state: %v", err)
			}
		}
	}

	schedule := func(root string, p ConfigPath) {
		mu.Lock()
		dirty[root] = true
		if t, ok := timers[root]; ok {
			t.Stop()
		}
		timers[root] = time.AfterFunc(watchDebounce, func() { fire(root, p) })
		mu.Unlock()
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer cancel()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-watcher.Events:
				if !ok {
					return
				}
				// Match the event path to one of our watched roots.
				for root, p := range rootByPath {
					if isUnder(ev.Name, root) {
						// On directory create, add it to the
						// watcher so we get future events inside.
						if ev.Op&fsnotify.Create != 0 {
							if info, err := os.Stat(ev.Name); err == nil && info.IsDir() {
								_ = watcher.Add(ev.Name)
							}
						}
						schedule(root, p)
						break
					}
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Printf("watch: fsnotify error: %v", err)
			}
		}
	}()

	// Safety scan ticker. fsnotify drops events under load (kernel
	// buffer overflow on Linux, FSEvents coalescing on macOS); the
	// scan catches anything the events missed.
	safety := time.NewTicker(watchSafetyScan)
	defer safety.Stop()

	log.Printf("watch: monitoring %d path(s) (debounce %s, safety scan every %s)",
		len(watchPaths), watchDebounce, watchSafetyScan)

	for {
		select {
		case <-ctx.Done():
			log.Printf("watch: shutting down")
			// Flush any pending debounces before exit.
			mu.Lock()
			for _, t := range timers {
				t.Stop()
			}
			pending := dirty
			dirty = map[string]bool{}
			mu.Unlock()
			for root := range pending {
				if p, ok := rootByPath[root]; ok && pending[root] {
					_, _ = syncPath(context.Background(), client, state, p, opts)
				}
			}
			if err := saveSyncState(state); err != nil {
				log.Printf("watch: save state on shutdown: %v", err)
			}
			return nil
		case <-safety.C:
			for root, p := range rootByPath {
				dropped, _ := syncPath(context.Background(), client, state, p, opts)
				if dropped > 0 {
					log.Printf("watch: safety scan %s -> %d dropped", root, dropped)
				}
			}
			_ = saveSyncState(state)
		}
	}
}

// addRecursive adds root and every existing subdirectory to the
// watcher. New subdirs created later are added on-the-fly via the
// fsnotify.Create event handler in runWatch's event loop.
func addRecursive(w *fsnotify.Watcher, root string) error {
	return filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			return nil
		}
		// Skip hidden dirs (.git, .cache, etc.) — same filter as
		// the sync walker.
		if p != root && d.Name() != "." && d.Name()[0] == '.' {
			return filepath.SkipDir
		}
		return w.Add(p)
	})
}

// isUnder reports whether p is inside root (or equals root).
func isUnder(p, root string) bool {
	rel, err := filepath.Rel(root, p)
	if err != nil {
		return false
	}
	if rel == "." {
		return true
	}
	return rel != "" && rel[0] != '.'
}
