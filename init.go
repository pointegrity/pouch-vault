package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

// runInit is `pouch-anchor init` — sets up the OS-conventional
// config + data directories, drops a stub anchor.env config the
// user can edit, and tells them what to do next. Idempotent: if
// the dirs / file already exist it leaves them alone (so re-running
// after a partial install is safe).
func runInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	force := fs.Bool("force", false, "overwrite anchor.env if it already exists")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfgDir, err := configDir()
	if err != nil {
		return fmt.Errorf("resolve config dir: %w", err)
	}
	dataDir, err := dataDir()
	if err != nil {
		return fmt.Errorf("resolve data dir: %w", err)
	}
	cfgPath := filepath.Join(cfgDir, "anchor.env")
	dbPath := filepath.Join(dataDir, "drops.db")

	if err := os.MkdirAll(cfgDir, 0o700); err != nil {
		return fmt.Errorf("mkdir %s: %w", cfgDir, err)
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return fmt.Errorf("mkdir %s: %w", dataDir, err)
	}

	wroteConfig := false
	if _, err := os.Stat(cfgPath); err == nil && !*force {
		// Leave it alone.
	} else {
		// Mode 600 — the file holds the api key + hmac secret.
		stub := configStub(dbPath)
		if err := os.WriteFile(cfgPath, []byte(stub), 0o600); err != nil {
			return fmt.Errorf("write %s: %w", cfgPath, err)
		}
		wroteConfig = true
	}

	fmt.Fprintln(os.Stderr, "pouch-anchor: scaffolded user-level install.")
	fmt.Fprintf(os.Stderr, "  config dir   %s\n", cfgDir)
	fmt.Fprintf(os.Stderr, "  data dir     %s\n", dataDir)
	fmt.Fprintf(os.Stderr, "  config file  %s%s\n", cfgPath,
		ternary(wroteConfig, "  (created)", "  (left alone — pass --force to overwrite)"))
	fmt.Fprintf(os.Stderr, "  database     %s\n", dbPath)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Next:")
	fmt.Fprintf(os.Stderr, "  1. Get an anchor key + secret from your pouch admin\n")
	fmt.Fprintf(os.Stderr, "     (`pouch anchor create --owner <you> --name <a-name>` on the server)\n")
	fmt.Fprintf(os.Stderr, "  2. Edit %s and fill in the values\n", cfgPath)
	fmt.Fprintf(os.Stderr, "  3. Run `pouch-anchor` (no subcommand) — it'll pick up the config automatically\n")
	return nil
}

// configStub returns the contents of a freshly-scaffolded anchor.env.
// Comments mark each line as a TODO; the daemon refuses to start
// until the placeholders are replaced.
func configStub(dbPath string) string {
	return `# pouch-anchor — user config.
#
# Values come from running` + " `pouch anchor create --owner <U> --name <N>` " + `
# on your pouch server (see https://pouch.pointegrity.com/docs/anchors).
# Uncomment and fill in the three REPLACE_ME values, then run pouch-anchor.

# Required.
POUCH_URL=https://pouch.pointegrity.com
POUCH_ANCHOR_KEY=REPLACE_ME
POUCH_HMAC_SECRET=REPLACE_ME

# Required for now: the URL pouch uses to reach this anchor over HTTP.
# Will become optional in the next release once SSE pull lands —
# anchors behind NAT will work without any tunneling.
POUCH_PUBLIC_URL=https://anchor.example/hook

# Storage. Default is OS-conventional (XDG on Linux, Library on
# macOS, AppData on Windows). Override here if you want it elsewhere.
ANCHOR_DB=` + dbPath + `

# Local listener — keep on loopback if you front it with a tunnel
# (cloudflared / tailscale funnel). Bind to :7780 directly only if
# this box is publicly reachable.
ANCHOR_LISTEN=127.0.0.1:7780

# Optional. Defaults to the OS hostname.
# ANCHOR_NAME=
`
}

func ternary(b bool, t, f string) string {
	if b {
		return t
	}
	return f
}
