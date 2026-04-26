package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// runGet fetches one drop by id.
//
//	pouch get itm-...                 # body to stdout
//	pouch get itm-... --json          # whole record as JSON to stdout
//	pouch get itm-... -o file.md      # body to file
func runGet(args []string) error {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	jsonOut := fs.Bool("json", false, "output the whole record as JSON")
	outFile := fs.String("o", "", "write body to FILE instead of stdout")
	server := fs.String("server", "", "pouch server URL (overrides POUCH_URL / config)")
	cfgPath := fs.String("config", os.Getenv("POUCH_CONFIG"), "config file path")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("usage: pouch get <itm-id>")
	}
	id := fs.Arg(0)

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
	tok, err := loadToken()
	if err != nil {
		return err
	}
	if tok == "" {
		return errors.New("not logged in. Run `pouch login --user <you>` first")
	}

	url := strings.TrimRight(cfg.URL, "/") + "/api/items/" + id
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("User-Agent", "pouch-cli/"+Version)

	cl := &http.Client{Timeout: 30 * time.Second}
	resp, err := cl.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return fmt.Errorf("not found: %s", id)
	}
	if resp.StatusCode == 401 {
		return errors.New("token rejected (401). Run `pouch login` again")
	}
	if resp.StatusCode >= 400 {
		buf, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("get: %d %s", resp.StatusCode, strings.TrimSpace(string(buf)))
	}

	if *jsonOut {
		if _, err := io.Copy(os.Stdout, resp.Body); err != nil {
			return err
		}
		return nil
	}

	// /api/items/{id} returns {"item": {...}, "children": [...]} —
	// extract the item.body for stdout.
	var rec struct {
		Item struct {
			Body string `json:"body"`
		} `json:"item"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	if *outFile != "" {
		if err := os.WriteFile(*outFile, []byte(rec.Item.Body), 0o644); err != nil {
			return fmt.Errorf("write %s: %w", *outFile, err)
		}
		fmt.Fprintf(os.Stderr, "wrote %d bytes to %s\n", len(rec.Item.Body), *outFile)
		return nil
	}
	if _, err := os.Stdout.Write([]byte(rec.Item.Body)); err != nil {
		return err
	}
	return nil
}
