package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

// runInit creates the OS-conventional config dir for the pouch CLI
// and drops a stub config.env. Idempotent unless --force.
func runInit(args []string) error {
	fs := flag.NewFlagSet("init", flag.ExitOnError)
	force := fs.Bool("force", false, "overwrite config.env if it already exists")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfgDir, err := configDir()
	if err != nil {
		return fmt.Errorf("resolve config dir: %w", err)
	}
	if err := os.MkdirAll(cfgDir, 0o700); err != nil {
		return fmt.Errorf("mkdir %s: %w", cfgDir, err)
	}
	cfgPath := filepath.Join(cfgDir, "config.env")

	wrote := false
	if _, err := os.Stat(cfgPath); err == nil && !*force {
		// Leave it alone.
	} else {
		stub := "# pouch CLI config — values used by 'pouch put'.\n" +
			"#\n" +
			"# Get an ingress key from your pouch admin:\n" +
			"#   pouch key create --owner <your-user-id> --label <a-name-you-pick>\n" +
			"# (Run on the pouch SERVER, by an admin.) The plaintext key is shown ONCE.\n" +
			"\n" +
			"POUCH_URL=https://pouch.pointegrity.com\n" +
			"POUCH_KEY=REPLACE_ME\n"
		if err := os.WriteFile(cfgPath, []byte(stub), 0o600); err != nil {
			return fmt.Errorf("write %s: %w", cfgPath, err)
		}
		wrote = true
	}

	fmt.Fprintln(os.Stderr, "pouch CLI: scaffolded user config.")
	fmt.Fprintf(os.Stderr, "  config dir   %s\n", cfgDir)
	fmt.Fprintf(os.Stderr, "  config file  %s%s\n", cfgPath,
		ternary(wrote, "  (created)", "  (left alone — pass --force to overwrite)"))
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Next:")
	fmt.Fprintf(os.Stderr, "  1. Get an ingress key from your pouch admin\n")
	fmt.Fprintf(os.Stderr, "     (`pouch key create --owner <you> --label <a-name>` on the server)\n")
	fmt.Fprintf(os.Stderr, "  2. Edit %s and replace REPLACE_ME with the key\n", cfgPath)
	fmt.Fprintf(os.Stderr, "  3. Try it: echo 'hello pouch' | pouch put --label 'first cli drop'\n")
	return nil
}

func ternary(b bool, t, f string) string {
	if b {
		return t
	}
	return f
}
