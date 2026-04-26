package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"golang.org/x/term"
)

// runLogin exchanges username/password for a JWT and saves it.
//
//	pouch login                       # prompts for both
//	pouch login --user jy             # prompts for password only
//	pouch login --user jy --server URL
//
// Once logged in, `pouch ls` / `pouch get` use the saved token via
// Authorization: Bearer. Logout = `rm <token-path>` (we'll add a
// `pouch logout` later if it earns the keystroke).
func runLogin(args []string) error {
	fs := flag.NewFlagSet("login", flag.ExitOnError)
	user := fs.String("user", "", "username")
	password := fs.String("password", "", "password (avoid; will appear in shell history / ps)")
	server := fs.String("server", "", "pouch server URL (overrides POUCH_URL / config)")
	cfgPath := fs.String("config", os.Getenv("POUCH_CONFIG"), "config file path")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := loadCLIConfig(*cfgPath)
	if err != nil {
		return err
	}
	if *server != "" {
		cfg.URL = *server
	}
	if cfg.URL == "" {
		return errors.New("POUCH_URL is required (set in env, --server, or config.env)")
	}

	if *user == "" {
		fmt.Fprint(os.Stderr, "username: ")
		var u string
		if _, err := fmt.Scanln(&u); err != nil {
			return fmt.Errorf("read username: %w", err)
		}
		*user = strings.TrimSpace(u)
	}
	if *password == "" {
		fmt.Fprint(os.Stderr, "password: ")
		pw, err := term.ReadPassword(int(syscall.Stdin))
		fmt.Fprintln(os.Stderr) // newline after the silent prompt
		if err != nil {
			return fmt.Errorf("read password: %w", err)
		}
		*password = string(pw)
	}

	body, _ := json.Marshal(map[string]string{
		"username": *user, "password": *password,
	})
	url := strings.TrimRight(cfg.URL, "/") + "/api/v1/auth/login"
	req, _ := http.NewRequest("POST", url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "pouch-cli/"+Version)

	cl := &http.Client{Timeout: 30 * time.Second}
	resp, err := cl.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		buf, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("login: %d %s", resp.StatusCode, strings.TrimSpace(string(buf)))
	}
	var out struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	if out.Token == "" {
		return errors.New("server returned empty token")
	}

	tp, err := tokenPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(tp), 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(tp, []byte(out.Token), 0o600); err != nil {
		return fmt.Errorf("save token: %w", err)
	}
	fmt.Fprintf(os.Stderr, "logged in as %s; token saved to %s\n", *user, tp)
	return nil
}

// tokenPath returns the on-disk location for the saved JWT.
func tokenPath() (string, error) {
	d, err := configDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(d, "token"), nil
}

// loadToken returns the saved JWT, or "" if none. Empty file → "".
func loadToken() (string, error) {
	tp, err := tokenPath()
	if err != nil {
		return "", err
	}
	b, err := os.ReadFile(tp)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}
