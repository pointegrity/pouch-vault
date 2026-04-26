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
	c := &config{
		pouchURL:   os.Getenv("POUCH_URL"),
		anchorKey:  os.Getenv("POUCH_ANCHOR_KEY"),
		hmacSecret: os.Getenv("POUCH_HMAC_SECRET"),
		publicURL:  os.Getenv("POUCH_PUBLIC_URL"),
		dbPath:     envOr("ANCHOR_DB", "drops.db"),
		listenAddr: envOr("ANCHOR_LISTEN", ":7780"),
		name:       os.Getenv("ANCHOR_NAME"),
		heartbeat:  30 * time.Second,
	}

	// CLI flags override env. Useful for local dev.
	flag.StringVar(&c.pouchURL, "pouch-url", c.pouchURL, "pouch SaaS base URL")
	flag.StringVar(&c.anchorKey, "anchor-key", c.anchorKey, "anchor API key (pk_...)")
	flag.StringVar(&c.hmacSecret, "hmac-secret", c.hmacSecret, "HMAC shared secret for webhook delivery")
	flag.StringVar(&c.publicURL, "public-url", c.publicURL, "where pouch can reach us, e.g. https://anchor.example/hook")
	flag.StringVar(&c.dbPath, "db", c.dbPath, "sqlite database path")
	flag.StringVar(&c.listenAddr, "addr", c.listenAddr, "listen address")
	flag.StringVar(&c.name, "name", c.name, "anchor name (defaults to hostname)")
	flag.DurationVar(&c.heartbeat, "heartbeat", c.heartbeat, "heartbeat interval")
	flag.Parse()

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

func main() {
	if err := run(); err != nil {
		log.Fatalf("pouch-anchor: %v", err)
	}
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
