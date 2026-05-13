// pouch-vault — Shape A: headless local relay daemon for pouch
// (https://pouch.pointegrity.com).
//
// What it does, in one paragraph: receives webhook deliveries from a
// pouch SaaS that knows about it, verifies the HMAC, persists each
// drop into a local SQLite database, and periodically heartbeats back
// to pouch with stats (last drop seen, total drops, hostname,
// version). The server side of this protocol lives in the pouch
// repository; provisioning is a one-time `pouch vault create
// --owner <U> --name <N>` admin step which mints the API key + HMAC
// secret you copy into the vault's config.
//
// Usage (env-var driven; all required):
//
//	POUCH_URL          https://pouch.pointegrity.com
//	POUCH_VAULT_KEY   pk_...                    # from `pouch vault create`
//	POUCH_HMAC_SECRET  abcdef...                 # from `pouch vault create`
//	POUCH_PUBLIC_URL   https://vault.example/hook   # how pouch reaches us
//	VAULT_DB          /var/lib/pouch-vault/drops.db
//	VAULT_LISTEN      :7780
//	VAULT_NAME        jy-laptop                 # optional; defaults to hostname
//
// Built for systemd. See examples/pouch-vault.service.
package main

import (
	"context"
	"encoding/json"
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
// panel can flag stale vault builds. Bump on release.
const Version = "0.8.0"

type config struct {
	pouchURL    string
	vaultKey   string
	hmacSecret  string
	publicURL   string
	dbPath      string
	blobsDir    string
	mirrorDir   string
	listenAddr  string
	name        string
	heartbeat   time.Duration
	// paths is the multi-folder declaration this vault binary
	// owns. Each entry is a {path, stream, label?}. The vault
	// sends paths[] on register + heartbeat; the cloud
	// reconciles channels from them per decision
	// vault-declares-paths-cloud-reflects-channels. Empty list
	// is allowed — vault pairs but routes nothing until paths
	// are configured.
	paths       []ConfigPath
}

// ConfigPath is one entry parsed out of VAULT_PATHS. JSON shape:
//   [{"path":"scrapes/","stream":"trip","direction":"watch"}]
//
// Direction controls which data flow uses this entry:
//   "mirror" (default) — cloud delivers drops here (SSE-mirror flow).
//   "watch"            — vault scans this folder for new/changed
//                        files and POSTs them as drops via
//                        `pouch-vault sync` / `watch` subcommands.
// Per decision vault-producer-mode-and-local-only-git.
type ConfigPath struct {
	Path      string `json:"path"`
	Stream    string `json:"stream"`
	Label     string `json:"label,omitempty"`
	Direction string `json:"direction,omitempty"`
}

// effectiveDirection returns the direction with the default applied.
func (p ConfigPath) effectiveDirection() string {
	if p.Direction == "" {
		return "mirror"
	}
	return p.Direction
}

func loadConfig() (*config, error) {
	// Resolve --config / $POUCH_VAULT_CONFIG first so the file's
	// values can seed env vars that aren't already set. Then the
	// per-field reads below pick up either the env (already there)
	// or the file (loaded into env).
	cfgFile := os.Getenv("POUCH_VAULT_CONFIG")
	flag.StringVar(&cfgFile, "config", cfgFile, "config file path (env-style KEY=VALUE)")
	// We need the --config flag to be parsed before we read other
	// flags / env, but flag.Parse() finalises the whole flag set.
	// Workaround: reuse Go's flag.CommandLine but call Parse early
	// by registering everything first.

	c := &config{}

	flag.StringVar(&c.pouchURL, "pouch-url", "", "pouch SaaS base URL")
	flag.StringVar(&c.vaultKey, "vault-key", "", "vault API key (pk_...)")
	flag.StringVar(&c.hmacSecret, "hmac-secret", "", "HMAC shared secret for webhook delivery")
	flag.StringVar(&c.publicURL, "public-url", "", "where pouch can reach us, e.g. https://vault.example/hook")
	flag.StringVar(&c.dbPath, "db", "", "sqlite database path")
	flag.StringVar(&c.blobsDir, "blobs", "", "directory for materialized binary blobs")
	flag.StringVar(&c.mirrorDir, "mirror", "", "directory for path-mirrored copies (streams with layout=mirror)")
	flag.StringVar(&c.listenAddr, "addr", "", "listen address")
	flag.StringVar(&c.name, "name", "", "vault name (defaults to hostname)")
	flag.DurationVar(&c.heartbeat, "heartbeat", 30*time.Second, "heartbeat interval")
	flag.Parse()

	// Now load the config file, with the resolution order:
	//   1. explicit --config / $POUCH_VAULT_CONFIG path
	//   2. <user-config-dir>/pouch-vault/vault.env
	//   3. /etc/pouch/vault.env
	// loadEnvFile is no-op on missing files and never overrides
	// existing env values, so env and CLI flags always win.
	candidates := []string{cfgFile}
	if userCfg, err := configPath(); err == nil {
		candidates = append(candidates, userCfg)
	}
	candidates = append(candidates, "/etc/pouch/vault.env")
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
	pickStr(&c.vaultKey,  "POUCH_VAULT_KEY", "")
	pickStr(&c.hmacSecret, "POUCH_HMAC_SECRET", "")
	pickStr(&c.publicURL,  "POUCH_PUBLIC_URL", "")
	if c.dbPath == "" {
		// Use OS-conventional default if neither env nor flag set it.
		if d, err := defaultDBPath(); err == nil {
			c.dbPath = envOr("VAULT_DB", d)
		} else {
			c.dbPath = envOr("VAULT_DB", "drops.db")
		}
	}
	if c.blobsDir == "" {
		// Default: sibling "blobs/" directory next to the DB. The
		// SQLite file lives in the OS data dir; blobs go right beside
		// so backups capture both at once.
		if d, err := dataDir(); err == nil {
			c.blobsDir = envOr("VAULT_BLOBS", filepath.Join(d, "blobs"))
		} else {
			c.blobsDir = envOr("VAULT_BLOBS", "blobs")
		}
	}
	if c.mirrorDir == "" {
		// Default: sibling "mirror/" directory next to blobs/. Drops
		// with stream.layout=mirror AND a non-empty original_path
		// also land here under the path the producer stamped, so
		// the user can browse with native tools.
		if d, err := dataDir(); err == nil {
			c.mirrorDir = envOr("VAULT_MIRROR", filepath.Join(d, "mirror"))
		} else {
			c.mirrorDir = envOr("VAULT_MIRROR", "mirror")
		}
	}
	pickStr(&c.listenAddr, "VAULT_LISTEN", ":7780")
	pickStr(&c.name,       "VAULT_NAME", "")

	// VAULT_PATHS is a JSON array of {path, stream, label?}. Parsed
	// once at boot; the resolved list rides on every register and
	// heartbeat so the cloud can reconcile channels (decision
	// vault-declares-paths-cloud-reflects-channels). Empty / unset
	// is fine — vault pairs but routes nothing.
	if raw := os.Getenv("VAULT_PATHS"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &c.paths); err != nil {
			return nil, fmt.Errorf("VAULT_PATHS: %w", err)
		}
		for i, p := range c.paths {
			if p.Path == "" {
				return nil, fmt.Errorf("VAULT_PATHS[%d]: path required", i)
			}
			if p.Stream == "" {
				return nil, fmt.Errorf("VAULT_PATHS[%d]: stream required", i)
			}
		}
	}

	if c.name == "" {
		if h, err := os.Hostname(); err == nil {
			c.name = h
		} else {
			c.name = "vault"
		}
	}
	c.pouchURL = strings.TrimRight(c.pouchURL, "/")

	// Required regardless of mode.
	for _, m := range []struct {
		val, name string
	}{
		{c.pouchURL, "POUCH_URL"},
		{c.vaultKey, "POUCH_VAULT_KEY"},
		{c.hmacSecret, "POUCH_HMAC_SECRET"},
	} {
		if m.val == "" {
			return nil, fmt.Errorf("%s is required", m.name)
		}
	}
	// POUCH_PUBLIC_URL is now OPTIONAL: set → push mode (we listen
	// on VAULT_LISTEN for /hook deliveries from pouch). Unset →
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
	// `pouch-vault init --force` doesn't try to feed --force to the
	// daemon's flag set.
	if len(os.Args) >= 2 {
		switch os.Args[1] {
		case "init":
			if err := runInit(os.Args[2:]); err != nil {
				log.Fatalf("pouch-vault init: %v", err)
			}
			return
		case "pair":
			if err := runPair(os.Args[2:]); err != nil {
				log.Fatalf("pouch-vault pair: %v", err)
			}
			return
		case "sync":
			if err := runSync(os.Args[2:]); err != nil {
				log.Fatalf("pouch-vault sync: %v", err)
			}
			return
		case "watch":
			if err := runWatch(os.Args[2:]); err != nil {
				log.Fatalf("pouch-vault watch: %v", err)
			}
			return
		case "history":
			if err := runHistory(os.Args[2:]); err != nil {
				log.Fatalf("pouch-vault history: %v", err)
			}
			return
		case "get":
			if err := runGet(os.Args[2:]); err != nil {
				log.Fatalf("pouch-vault get: %v", err)
			}
			return
		case "version", "--version", "-v":
			fmt.Printf("pouch-vault %s\n", Version)
			return
		case "help", "--help", "-h":
			printHelp()
			return
		}
	}

	if err := run(); err != nil {
		log.Fatalf("pouch-vault: %v", err)
	}
}

func printHelp() {
	fmt.Fprint(os.Stderr, `pouch-vault — local relay daemon for pouch.

Usage:
  pouch-vault                   run the daemon (reads config from env / file)
  pouch-vault pair              first-boot: exchange a pairing key for
                                long-lived credentials (printed once)
  pouch-vault sync [--dry]      one-shot: scan every VAULT_PATHS entry with
                                direction='watch' and drop new/changed files
                                into pouch. Schedule via cron.
  pouch-vault watch             long-running daemon: fsnotify-driven
                                producer. Coalesces rapid-fire events with
                                a 500ms debounce; falls back to a 30s
                                safety scan.
  pouch-vault init [--force]    scaffold OS-conventional config + data dirs
  pouch-vault version           print version and exit
  pouch-vault help              print this help

Required (both modes):
  POUCH_URL          --pouch-url     pouch SaaS base URL
  POUCH_VAULT_KEY   --vault-key    vault API key
  POUCH_HMAC_SECRET  --hmac-secret   delivery signature secret

Optional:
  POUCH_PUBLIC_URL   --public-url    enables PUSH mode — pouch POSTs to this URL.
                                     Unset (default) = PULL mode: vault opens an
                                     SSE connection to pouch. Pull mode needs no
                                     publicly-reachable endpoint, no firewall hole,
                                     no tunneling. Recommended for most users.
  VAULT_DB          --db            sqlite database path
  VAULT_LISTEN      --addr          local listener (push mode: receives /hook;
                                     pull mode: just /healthz; "off" disables)
  VAULT_NAME        --name          vault name (defaults to hostname)
                     --heartbeat     heartbeat interval (default 30s)
                     --config        explicit config file path

The daemon also reads an env-style config file. Lookup order:
  1. --config <path> / $POUCH_VAULT_CONFIG
  2. <user-config-dir>/pouch-vault/vault.env
       (Linux: ~/.config; macOS: ~/Library/Application Support; Windows: %AppData%)
  3. /etc/pouch/vault.env (system-wide)
File values fill in any env var that's NOT already set — env / flags always win.

Provisioning:
  pouch vault create --owner <U> --name <N>     (admin shell on pouch server)
  pouch-vault init                              (here, on the vault host)
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

	// Outbound client (vault → pouch). Used immediately for register
	// + every heartbeat thereafter (and, in pull mode, for the SSE
	// stream).
	client := NewPouchClient(cfg.pouchURL, cfg.vaultKey)

	// Register with pouch. In pull mode publicURL is empty, which
	// pouch v0.2+ accepts (it tells the dispatcher to route via the
	// SSE hub instead of HTTP POST). The declared paths[] from
	// VAULT_PATHS rides along so the cloud reconciles channels
	// before the first heartbeat (decision
	// vault-declares-paths-cloud-reflects-channels).
	regPaths := make([]RegisterPath, 0, len(cfg.paths))
	for _, p := range cfg.paths {
		regPaths = append(regPaths, RegisterPath{
			Path:   p.Path,
			Stream: p.Stream,
			Label:  p.Label,
		})
	}
	regCtx, regCancel := context.WithTimeout(context.Background(), 10*time.Second)
	vaultID, err := client.Register(regCtx, cfg.publicURL, hostnameOr("vault"), Version, regPaths)
	regCancel()
	if err != nil {
		return fmt.Errorf("register with pouch: %w", err)
	}

	mode := "pull"
	if cfg.publicURL != "" {
		mode = "push"
	}
	log.Printf("registered as %s (name=%s, mode=%s)", vaultID, cfg.name, mode)

	// Initialize the shared status singleton — used by the local UI
	// + the daemon's introspection.
	status.VaultName = cfg.name
	status.VaultID = vaultID
	status.Version = Version
	status.Mode = mode
	status.Hostname = hostnameOr("vault")
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
	go runHeartbeats(ctx, client, store, cfg.heartbeat, cfg.paths)

	// Chunked-download consumer (Phase 5 slice 8e.consumer). Shared
	// by SSE pull mode and HTTP push mode. Runs a background loop
	// that drains downloads.json — both blob-arrival events from
	// fresh SSE/POST and incomplete entries from a previous run.
	dl := NewDownloader(client, store, cfg.blobsDir, cfg.mirrorDir,
		downloaderStatePath(), DownloadChunkSize,
		downloadThrottleFromEnv())
	dl.Start(ctx)

	if mode == "pull" {
		// Pull mode: no inbound /hook needed. Still useful to serve
		// /healthz + the local UI on VAULT_LISTEN. Skip if the user
		// explicitly set VAULT_LISTEN=off.
		if cfg.listenAddr != "" && cfg.listenAddr != "off" {
			go runLocalListener(ctx, cfg.listenAddr, store, cfg.blobsDir)
		}
		log.Printf("pouch-vault %s pull-mode, db=%s, blobs=%s, mirror=%s, pouch=%s",
			Version, cfg.dbPath, cfg.blobsDir, cfg.mirrorDir, cfg.pouchURL)
		if cfg.listenAddr != "off" {
			log.Printf("local UI: http://%s/ui", normalizeListenForURL(cfg.listenAddr))
		}
		// Inline producer loop: if VAULT_PATHS has any direction='watch'
		// entries, run the watch loop as a goroutine alongside the SSE
		// consumer. One daemon serves both roles — keeps launchd/systemd
		// unit count to one and the local UI on the same port.
		// Phase 5 slice 8e (post-launchd).
		var watchPaths []ConfigPath
		for _, p := range cfg.paths {
			if p.effectiveDirection() == "watch" {
				watchPaths = append(watchPaths, p)
			}
		}
		if len(watchPaths) > 0 {
			st, err := loadSyncState()
			if err != nil {
				log.Printf("pull-mode watch: state load: %v — starting fresh", err)
				st = &syncState{Paths: map[string]*syncStatePath{}}
			}
			uploader := newUploaderFromConfig(client, cfg)
			opts := syncOpts{maxInline: defaultMaxInline, uploader: uploader}
			go func() {
				if err := runWatchLoop(ctx, client, watchPaths, st, opts); err != nil {
					log.Printf("pull-mode watch: %v", err)
				}
			}()
			log.Printf("watch loop: monitoring %d path(s) in pull-mode process", len(watchPaths))
		}
		runStream(ctx, client, store, cfg.hmacSecret, cfg.blobsDir, cfg.mirrorDir, dl)
		log.Printf("pouch-vault: stopped")
		return nil
	}

	// Push mode: pouch POSTs /hook deliveries to us. Same shape as
	// the regular webhook receiver (HMAC verify + dedup + insert).
	// We still mount the local UI on the same listener.
	receiver := NewReceiver(store, cfg.hmacSecret, cfg.blobsDir, cfg.mirrorDir, dl)
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

	log.Printf("pouch-vault %s push-mode, listening on %s, db=%s, pouch=%s",
		Version, cfg.listenAddr, cfg.dbPath, cfg.pouchURL)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	log.Printf("pouch-vault: stopped")
	return nil
}

// runLocalListener serves the local-UI surface (status, recent
// drops, viewer HTML) plus /healthz on addr. Used in pull mode where
// there's no /hook. The mux comes pre-wired by mountLocalUI; the
// extra arg is for future use (push-mode receivers, vault-only
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
