package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// CLIConfig holds the values pouch CLI needs at runtime.
type CLIConfig struct {
	URL string // pouch SaaS base URL
	Key string // ingress API key (POUCH_KEY)
}

// loadCLIConfig resolves config from (in priority order):
//   1. environment variables (POUCH_URL, POUCH_KEY)
//   2. file at explicit path (--config / $POUCH_CONFIG), if given
//   3. file at <user-config-dir>/pouch/config.env
//   4. file at /etc/pouch/cli.env
// File values fill in env vars NOT already set; explicit env always wins.
func loadCLIConfig(explicit string) (*CLIConfig, error) {
	candidates := []string{explicit}
	if d, err := configDir(); err == nil {
		candidates = append(candidates, filepath.Join(d, "config.env"))
	}
	candidates = append(candidates, "/etc/pouch/cli.env")
	for _, p := range candidates {
		if p == "" {
			continue
		}
		if err := loadEnvFile(p); err != nil {
			return nil, fmt.Errorf("config %s: %w", p, err)
		}
	}
	return &CLIConfig{
		URL: os.Getenv("POUCH_URL"),
		Key: os.Getenv("POUCH_KEY"),
	}, nil
}

// configDir returns the user-level config dir for the pouch CLI.
// Distinct from the anchor's dir — the CLI is its own thing.
//
//	Linux:   $XDG_CONFIG_HOME/pouch or ~/.config/pouch
//	macOS:   ~/Library/Application Support/pouch
//	Windows: %AppData%\pouch
func configDir() (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		home, herr := os.UserHomeDir()
		if herr != nil {
			return "", err
		}
		return filepath.Join(home, ".pouch"), nil
	}
	return filepath.Join(base, "pouch"), nil
}

// loadEnvFile is the same KEY=VALUE parser the daemon uses. Comments
// (#) skipped; existing env values not overridden. Missing file =
// soft no-op.
func loadEnvFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		eq := strings.IndexByte(line, '=')
		if eq <= 0 {
			return fmt.Errorf("%s:%d: expected KEY=VALUE", path, lineNum)
		}
		key := strings.TrimSpace(line[:eq])
		val := strings.TrimSpace(line[eq+1:])
		if len(val) >= 2 {
			if (val[0] == '"' && val[len(val)-1] == '"') ||
				(val[0] == '\'' && val[len(val)-1] == '\'') {
				val = val[1 : len(val)-1]
			}
		}
		if _, present := os.LookupEnv(key); present {
			continue
		}
		_ = os.Setenv(key, val)
	}
	return scanner.Err()
}

