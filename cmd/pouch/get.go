package main

import (
	"encoding/base64"
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
	// extract the bytes correctly per encoding:
	//   utf8   → write item.body verbatim
	//   base64 → decode item.body
	//   blob   → fetch /api/blobs/<id> separately
	var rec struct {
		Item struct {
			Body         string `json:"body"`
			BodyEncoding string `json:"body_encoding,omitempty"`
			BodyBlobID   string `json:"body_blob_id,omitempty"`
			MIME         string `json:"mime,omitempty"`
		} `json:"item"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rec); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	var out []byte
	switch rec.Item.BodyEncoding {
	case "blob":
		if rec.Item.BodyBlobID == "" {
			return errors.New("blob-encoded item has no blob id")
		}
		blobURL := strings.TrimRight(cfg.URL, "/") + "/api/blobs/" + rec.Item.BodyBlobID
		req2, _ := http.NewRequest("GET", blobURL, nil)
		req2.Header.Set("Authorization", "Bearer "+tok)
		req2.Header.Set("User-Agent", "pouch-cli/"+Version)
		r2, err := cl.Do(req2)
		if err != nil {
			return fmt.Errorf("fetch blob: %w", err)
		}
		defer r2.Body.Close()
		if r2.StatusCode >= 400 {
			buf, _ := io.ReadAll(io.LimitReader(r2.Body, 1024))
			return fmt.Errorf("fetch blob %d: %s", r2.StatusCode, strings.TrimSpace(string(buf)))
		}
		out, err = io.ReadAll(r2.Body)
		if err != nil {
			return err
		}
	case "base64":
		decoded, err := base64.StdEncoding.DecodeString(rec.Item.Body)
		if err != nil {
			return fmt.Errorf("decode base64 body: %w", err)
		}
		out = decoded
	default:
		out = []byte(rec.Item.Body)
	}
	if *outFile != "" {
		if err := os.WriteFile(*outFile, out, 0o644); err != nil {
			return fmt.Errorf("write %s: %w", *outFile, err)
		}
		note := ""
		switch rec.Item.BodyEncoding {
		case "base64":
			note = " (decoded from base64)"
		case "blob":
			note = " (streamed from blob storage)"
		}
		fmt.Fprintf(os.Stderr, "wrote %d bytes to %s%s\n", len(out), *outFile, note)
		return nil
	}
	// To stdout — only safe if it's text or the user is piping somewhere.
	// We do not gate this; if you `pouch get image-id` to a tty you
	// asked for that. Pipe to a file or use -o for binary.
	if _, err := os.Stdout.Write(out); err != nil {
		return err
	}
	return nil
}

