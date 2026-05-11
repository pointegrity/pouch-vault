package main

import (
	"os"
	"path/filepath"
	"runtime"
)

// configDir returns the user-level config directory for pouch-vault,
// per OS convention:
//
//	Linux:   $XDG_CONFIG_HOME/pouch-vault or ~/.config/pouch-vault
//	macOS:   ~/Library/Application Support/pouch-vault
//	Windows: %AppData%\pouch-vault
//
// Falls back to ~/.pouch-vault if the OS lookup fails (which only
// happens with truly broken environments — no $HOME etc).
func configDir() (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		home, herr := os.UserHomeDir()
		if herr != nil {
			return "", err
		}
		return filepath.Join(home, ".pouch-vault"), nil
	}
	return filepath.Join(base, "pouch-vault"), nil
}

// dataDir returns the user-level data directory — distinct from
// config on Linux (XDG separates them) but the same on macOS /
// Windows because those OSes don't have the distinction. The DB
// goes here.
//
//	Linux:   $XDG_DATA_HOME/pouch-vault or ~/.local/share/pouch-vault
//	macOS:   ~/Library/Application Support/pouch-vault
//	Windows: %AppData%\pouch-vault
func dataDir() (string, error) {
	if runtime.GOOS == "linux" {
		if d := os.Getenv("XDG_DATA_HOME"); d != "" {
			return filepath.Join(d, "pouch-vault"), nil
		}
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		return filepath.Join(home, ".local", "share", "pouch-vault"), nil
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
	return filepath.Join(d, "vault.env"), nil
}

// defaultDBPath returns the canonical database path inside dataDir.
func defaultDBPath() (string, error) {
	d, err := dataDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(d, "drops.db"), nil
}
