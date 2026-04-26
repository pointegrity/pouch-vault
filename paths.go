package main

import (
	"os"
	"path/filepath"
	"runtime"
)

// configDir returns the user-level config directory for pouch-anchor,
// per OS convention:
//
//	Linux:   $XDG_CONFIG_HOME/pouch-anchor or ~/.config/pouch-anchor
//	macOS:   ~/Library/Application Support/pouch-anchor
//	Windows: %AppData%\pouch-anchor
//
// Falls back to ~/.pouch-anchor if the OS lookup fails (which only
// happens with truly broken environments — no $HOME etc).
func configDir() (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		home, herr := os.UserHomeDir()
		if herr != nil {
			return "", err
		}
		return filepath.Join(home, ".pouch-anchor"), nil
	}
	return filepath.Join(base, "pouch-anchor"), nil
}

// dataDir returns the user-level data directory — distinct from
// config on Linux (XDG separates them) but the same on macOS /
// Windows because those OSes don't have the distinction. The DB
// goes here.
//
//	Linux:   $XDG_DATA_HOME/pouch-anchor or ~/.local/share/pouch-anchor
//	macOS:   ~/Library/Application Support/pouch-anchor
//	Windows: %AppData%\pouch-anchor
func dataDir() (string, error) {
	if runtime.GOOS == "linux" {
		if d := os.Getenv("XDG_DATA_HOME"); d != "" {
			return filepath.Join(d, "pouch-anchor"), nil
		}
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		return filepath.Join(home, ".local", "share", "pouch-anchor"), nil
	}
	// macOS + Windows reuse the config dir for app data.
	return configDir()
}

// configPath returns the canonical config file path inside configDir.
func configPath() (string, error) {
	d, err := configDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(d, "anchor.env"), nil
}

// defaultDBPath returns the canonical database path inside dataDir.
func defaultDBPath() (string, error) {
	d, err := dataDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(d, "drops.db"), nil
}
