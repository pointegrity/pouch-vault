// pouch-anchor — Shape A: headless local relay daemon for pouch
// (https://pouch.pointegrity.com).
//
// What it does, in one paragraph: receives webhook deliveries from a
// pouch SaaS that knows about it, verifies the HMAC, persists each
// drop into a local SQLite database, and periodically heartbeats back
// to pouch with stats (last drop seen, total drops, hostname,
// version). The server side of this protocol lives in the pouch
// repository; provisioning is a one-time `pouch anchor create
// --owner <U> --name <N>` admin step which mints the API key + HMAC
// secret you copy into the anchor's config.
//
// Usage (env-var driven; all required):
//
//	POUCH_URL          https://pouch.pointegrity.com
//	POUCH_ANCHOR_KEY   pk_...                    # from `pouch anchor create`
//	POUCH_HMAC_SECRET  abcdef...                 # from `pouch anchor create`
//	POUCH_PUBLIC_URL   https://anchor.example/hook   # how pouch reaches us
//	ANCHOR_DB          /var/lib/pouch-anchor/drops.db
//	ANCHOR_LISTEN      :7780
//	ANCHOR_NAME        jy-laptop                 # optional; defaults to hostname
//
// Built for systemd. See examples/pouch-anchor.service.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// Version is stamped into heartbeats so the SaaS replication-status
// panel can flag stale anchor builds. Bump on release.
const Version = "0.7.0"

type config struct {
	pouchURL    string
	anchorKey   string
	hmacSecret  string
	publicURL   string
	dbPath      string
	blobsDir    string
	listenAddr  string
	name        string
	heartbeat   time.Duration
}

func loadConfig() (*config, error) {
	// Resolve --config / $POUCH_ANCHOR_CONFIG first so the file's
	// values can seed env vars that aren't already set. Then the
	// per-field reads below pick up either the env (already there)
	// or the file (loaded into env).
	cfgFile := os.Getenv("POUCH_ANCHOR_CONFIG")
	flag.StringVar(&cfgFile, "config", cfgFile, "config file path (env-style KEY=VALUE)")
	// We need the --config flag to be parsed before we read other
	// flags / env, but flag.Parse() finalises the whole flag set.
	// Workaround: reuse Go's flag.CommandLine but call Parse early
	// by registering everything first.

	c := &config{}

	flag.StringVar(&c.pouchURL, "pouch-url", "", "pouch SaaS base URL")
	flag.StringVar(&c.anchorKey, "anchor-key", "", "anchor API key (pk_...)")
	flag.StringVar(&c.hmacSecret, "hmac-secret", "", "HMAC shared secret for webhook delivery")
	flag.StringVar(&c.publicURL, "public-url", "", "where pouch can reach us, e.g. https://anchor.example/hook")
	flag.StringVar(&c.dbPath, "db", "", "sqlite database path")
	flag.StringVar(&c.blobsDir, "blobs", "", "directory for materialized binary blobs")
	flag.StringVar(&c.listenAddr, "addr", "", "listen address")
	flag.StringVar(&c.name, "name", "", "anchor name (defaults to hostname)")
	flag.DurationVar(&c.heartbeat, "heartbeat", 30*time.Second, "heartbeat interval")
	flag.Parse()

	// Now load the config file, with the resolution order:
	//   1. explicit --config / $POUCH_ANCHOR_CONFIG path
	//   2. <user-config-dir>/pouch-anchor/anchor.env
	//   3. /etc/pouch/anchor.env
	// loadEnvFile is no-op on missing files and never overrides
	// existing env values, so env and CLI flags always win.
	candidates := []string{cfgFile}
	if userCfg, err := configPath(); err == nil {
		candidates = append(candidates, userCfg)
	}
	candidates = append(candidates, "/etc/pouch/anchor.env")
	for _, p := range candidates {
		if p == "" {
			continue
		}
		if err := loadEnvFile(p); err != nil {
			return nil, fmt.Errorf("config file %s: %w", p, err)
		}
	}

	// CLI flag wins; else env (now possibly seeded from a file);
	// else built-in default.
	pickStr(&c.pouchURL,   "POUCH_URL", "")
	pickStr(&c.anchorKey,  "POUCH_ANCHOR_KEY", "")
	pickStr(&c.hmacSecret, "POUCH_HMAC_SECRET", "")
	pickStr(&c.publicURL,  "POUCH_PUBLIC_URL", "")
	if c.dbPath == "" {
		// Use OS-conventional default if neither env nor flag set it.
		if d, err := defaultDBPath(); err == nil {
			c.dbPath = envOr("ANCHOR_DB", d)
		} else {
			c.dbPath = envOr("ANCHOR_DB", "drops.db")
		}
	}
	if c.blobsDir == "" {
		// Default: sibling "blobs/" directory next to the DB. The
		// SQLite file lives in the OS data dir; blobs go right beside
		// so backups capture both at once.
		if d, err := dataDir(); err == nil {
			c.blobsDir = envOr("ANCHOR_BLOBS", filepath.Join(d, "blobs"))
		} else {
			c.blobsDir = envOr("ANCHOR_BLOBS", "blobs")
		}
	}
	pickStr(&c.listenAddr, "ANCHOR_LISTEN", ":7780")
	pickStr(&c.name,       "ANCHOR_NAME", "")

	if c.name == "" {
		if h, err := os.Hostname(); err == nil {
			c.name = h
		} else {
			c.name = "anchor"
		}
	}
	c.pouchURL = strings.TrimRight(c.pouchURL, "/")

	// Required regardless of mode.
	for _, m := range []struct {
		val, name string
	}{
		{c.pouchURL, "POUCH_URL"},
		{c.anchorKey, "POUCH_ANCHOR_KEY"},
		{c.hmacSecret, "POUCH_HMAC_SECRET"},
	} {
		if m.val == "" {
			return nil, fmt.Errorf("%s is required", m.name)
		}
	}
	// POUCH_PUBLIC_URL is now OPTIONAL: set → push mode (we listen
	// on ANCHOR_LISTEN for /hook deliveries from pouch). Unset →
	// pull mode (we hold an SSE connection open to pouch). Pull
	// mode is the default and what most users want — no public URL,
	// no tunneling, no firewall holes.
	return c, nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// pickStr fills *dst from environment variable `env` if dst is
// currently empty (so an explicit --flag wins). Falls back to def.
func pickStr(dst *string, env, def string) {
	if *dst != "" {
		return
	}
	if v := os.Getenv(env); v != "" {
		*dst = v
		return
	}
	*dst = def
}

func main() {
	// Subcommands are matched on os.Args[1] before flag parsing so
	// `pouch-anchor init --force` doesn't try to feed --force to the
	// daemon's flag set.
	if len(os.Args) >= 2 {
		switch os.Args[1] {
		case "init":
			if err := runInit(os.Args[2:]); err != nil {
				log.Fatalf("pouch-anchor init: %v", err)
			}
			return
		case "version", "--version", "-v":
			fmt.Printf("pouch-anchor %s\n", Version)
			return
		case "help", "--help", "-h":
			printHelp()
			return
		}
	}

	if err := run(); err != nil {
		log.Fatalf("pouch-anchor: %v", err)
	}
}

func printHelp() {
	fmt.Fprint(os.Stderr, `pouch-anchor — local relay daemon for pouch.

Usage:
  pouch-anchor                   run the daemon (reads config from env / file)
  pouch-anchor init [--force]    scaffold OS-conventional config + data dirs
  pouch-anchor version           print version and exit
  pouch-anchor help              print this help

Required (both modes):
  POUCH_URL          --pouch-url     pouch SaaS base URL
  POUCH_ANCHOR_KEY   --anchor-key    anchor API key
  POUCH_HMAC_SECRET  --hmac-secret   delivery signature secret

Optional:
  POUCH_PUBLIC_URL   --public-url    enables PUSH mode — pouch POSTs to this URL.
                                     Unset (default) = PULL mode: anchor opens an
                                     SSE connection to pouch. Pull mode needs no
                                     publicly-reachable endpoint, no firewall hole,
                                     no tunneling. Recommended for most users.
  ANCHOR_DB          --db            sqlite database path
  ANCHOR_LISTEN      --addr          local listener (push mode: receives /hook;
                                     pull mode: just /healthz; "off" disables)
  ANCHOR_NAME        --name          anchor name (defaults to hostname)
                     --heartbeat     heartbeat interval (default 30s)
                     --config        explicit config file path

The daemon also reads an env-style config file. Lookup order:
  1. --config <path> / $POUCH_ANCHOR_CONFIG
  2. <user-config-dir>/pouch-anchor/anchor.env
       (Linux: ~/.config; macOS: ~/Library/Application Support; Windows: %AppData%)
  3. /etc/pouch/anchor.env (system-wide)
File values fill in any env var that's NOT already set — env / flags always win.

Provisioning:
  pouch anchor create --owner <U> --name <N>     (admin shell on pouch server)
  pouch-anchor init                              (here, on the anchor host)
`)
}

func run() error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	store, err := OpenStore(cfg.dbPath)
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	defer store.Close()

	// Outbound client (anchor → pouch). Used immediately for register
	// + every heartbeat thereafter (and, in pull mode, for the SSE
	// stream).
	client := NewPouchClient(cfg.pouchURL, cfg.anchorKey)

	// Register with pouch. In pull mode publicURL is empty, which
	// pouch v0.2+ accepts (it tells the dispatcher to route via the
	// SSE hub instead of HTTP POST).
	regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
	anchorID, err := client.Register(regCtx, cfg.publicURL, hostnameOr("anchor"), Version)
	regCancel()
	if err != nil {
		return fmt.Errorf("register with pouch: %w", err)
	}

	mode := "pull"
	if cfg.publicURL != "" {
		mode = "push"
	}
	log.Printf("registered as %s (name=%s, mode=%s)", anchorID, cfg.name, mode)

	// Initialize the shared status singleton — used by the local UI
	// + the daemon's introspection.
	status.AnchorName = cfg.name
	status.AnchorID = anchorID
	status.Version = Version
	status.Mode = mode
	status.Hostname = hostnameOr("anchor")
	status.PouchURL = cfg.pouchURL
	status.DBPath = cfg.dbPath
	status.StartedAt = time.Now().UTC()

	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Heartbeat runs in both modes. In pull mode, the SSE connection
	// is the primary "I'm alive" signal; the heartbeat additionally
	// reports last_drop_id and total_drops so the SaaS UI can show
	// archive progress.
	go runHeartbeats(ctx, client, store, cfg.heartbeat)

	if mode == "pull" {
		// Pull mode: no inbound /hook needed. Still useful to serve
		// /healthz + the local UI on ANCHOR_LISTEN. Skip if the user
		// explicitly set ANCHOR_LISTEN=off.
		if cfg.listenAddr != "" && cfg.listenAddr != "off" {
			go runLocalListener(ctx, cfg.listenAddr, store, cfg.blobsDir)
		}
		log.Printf("pouch-anchor %s pull-mode, db=%s, blobs=%s, pouch=%s",
			Version, cfg.dbPath, cfg.blobsDir, cfg.pouchURL)
		if cfg.listenAddr != "off" {
			log.Printf("local UI: http://%s/ui", normalizeListenForURL(cfg.listenAddr))
		}
		runStream(ctx, client, store, cfg.hmacSecret, cfg.blobsDir)
		log.Printf("pouch-anchor: stopped")
		return nil
	}

	// Push mode: pouch POSTs /hook deliveries to us. Same shape as
	// the regular webhook receiver (HMAC verify + dedup + insert).
	// We still mount the local UI on the same listener.
	receiver := NewReceiver(store, cfg.hmacSecret, cfg.blobsDir)
	mux := http.NewServeMux()
	mux.Handle("POST /hook", receiver.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mountLocalUI(mux, store, cfg.blobsDir)
	srv := &http.Server{Addr: cfg.listenAddr, Handler: mux}
	log.Printf("local UI: http://%s/ui", normalizeListenForURL(cfg.listenAddr))

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("pouch-anchor %s push-mode, listening on %s, db=%s, pouch=%s",
		Version, cfg.listenAddr, cfg.dbPath, cfg.pouchURL)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	log.Printf("pouch-anchor: stopped")
	return nil
}

// runLocalListener serves the local-UI surface (status, recent
// drops, viewer HTML) plus /healthz on addr. Used in pull mode where
// there's no /hook. The mux comes pre-wired by mountLocalUI; the
// extra arg is for future use (push-mode receivers, anchor-only
// admin endpoints, etc).
func runLocalListener(ctx context.Context, addr string, st *Store, blobsDir string) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	mountLocalUI(mux, st, blobsDir)
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("local listener: %v", err)
	}
}

// normalizeListenForURL turns a listen address into a host:port that
// works in a browser URL. ":7780" -> "localhost:7780"; "127.0.0.1:7780"
// stays as-is.
func normalizeListenForURL(addr string) string {
	if len(addr) > 0 && addr[0] == ':' {
		return "localhost" + addr
	}
	return addr
}

func hostnameOr(def string) string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return def
}
