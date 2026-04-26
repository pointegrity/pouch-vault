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
	"strings"
	"syscall"
	"time"
)

// Version is stamped into heartbeats so the SaaS replication-status
// panel can flag stale anchor builds. Bump on release.
const Version = "0.1.0"

type config struct {
	pouchURL    string
	anchorKey   string
	hmacSecret  string
	publicURL   string
	dbPath      string
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

	for _, m := range []struct {
		val, name string
	}{
		{c.pouchURL, "POUCH_URL"},
		{c.anchorKey, "POUCH_ANCHOR_KEY"},
		{c.hmacSecret, "POUCH_HMAC_SECRET"},
		{c.publicURL, "POUCH_PUBLIC_URL"},
	} {
		if m.val == "" {
			return nil, fmt.Errorf("%s is required", m.name)
		}
	}
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
	fmt.Fprintln(os.Stderr, `pouch-anchor — local relay daemon for pouch.

Usage:
  pouch-anchor                   run the daemon (reads config from env / file)
  pouch-anchor init [--force]    scaffold OS-conventional config + data dirs
  pouch-anchor version           print version and exit
  pouch-anchor help              print this help

Environment / flags for the daemon:
  POUCH_URL          --pouch-url     pouch SaaS base URL (required)
  POUCH_ANCHOR_KEY   --anchor-key    anchor API key (required)
  POUCH_HMAC_SECRET  --hmac-secret   delivery signature secret (required)
  POUCH_PUBLIC_URL   --public-url    where pouch reaches us (required)
  ANCHOR_DB          --db            sqlite database path
  ANCHOR_LISTEN      --addr          listen address
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
	// + every heartbeat thereafter.
	client := NewPouchClient(cfg.pouchURL, cfg.anchorKey)

	// Register with pouch before opening the listener — if pouch
	// rejects us (bad key, bad URL), we fail loudly at startup
	// instead of silently never getting any drops.
	regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
	anchorID, err := client.Register(regCtx, cfg.publicURL, hostnameOr("anchor"), Version)
	regCancel()
	if err != nil {
		return fmt.Errorf("register with pouch: %w", err)
	}
	log.Printf("registered as %s (name=%s, public_url=%s)", anchorID, cfg.name, cfg.publicURL)

	// Inbound HTTP server (pouch → anchor /hook).
	receiver := NewReceiver(store, cfg.hmacSecret)
	mux := http.NewServeMux()
	mux.Handle("POST /hook", receiver.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	srv := &http.Server{Addr: cfg.listenAddr, Handler: mux}

	// Lifecycle: heartbeat goroutine + HTTP server, both bound to ctx
	// so SIGTERM kills both cleanly.
	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go runHeartbeats(ctx, client, store, cfg.heartbeat)

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("pouch-anchor %s listening on %s, db=%s, pouch=%s",
		Version, cfg.listenAddr, cfg.dbPath, cfg.pouchURL)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	log.Printf("pouch-anchor: stopped")
	return nil
}

func hostnameOr(def string) string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return def
}
