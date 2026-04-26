package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"text/tabwriter"
	"time"
)

// runLs lists drops in your pouch with optional filters.
//
//	pouch ls                       # newest 25
//	pouch ls --stream kept
//	pouch ls --tag work --limit 50
//	pouch ls --query "release notes"
//	pouch ls --json
func runLs(args []string) error {
	fs := flag.NewFlagSet("ls", flag.ExitOnError)
	stream := fs.String("stream", "", "filter by stream")
	tag := fs.String("tag", "", "filter by tag")
	q := fs.String("query", "", "full-text search over label/body/tags")
	limit := fs.Int("limit", 25, "max rows")
	jsonOut := fs.Bool("json", false, "output JSON instead of a table")
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
	tok, err := loadToken()
	if err != nil {
		return err
	}
	if tok == "" {
		return errors.New("not logged in. Run `pouch login --user <you>` first")
	}

	v := url.Values{}
	if *stream != "" {
		v.Set("stream", *stream)
	}
	if *tag != "" {
		v.Set("tag", *tag)
	}
	if *q != "" {
		v.Set("q", *q)
	}
	if *limit > 0 {
		v.Set("limit", fmt.Sprintf("%d", *limit))
	}
	u := strings.TrimRight(cfg.URL, "/") + "/api/items"
	if len(v) > 0 {
		u += "?" + v.Encode()
	}
	req, _ := http.NewRequest("GET", u, nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("User-Agent", "pouch-cli/"+Version)

	cl := &http.Client{Timeout: 30 * time.Second}
	resp, err := cl.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 401 {
		return errors.New("token rejected (401). Run `pouch login` again")
	}
	if resp.StatusCode >= 400 {
		buf, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("ls: %d %s", resp.StatusCode, strings.TrimSpace(string(buf)))
	}

	if *jsonOut {
		// Stream straight to stdout — no decode + re-encode.
		if _, err := io.Copy(os.Stdout, resp.Body); err != nil {
			return err
		}
		return nil
	}

	var out struct {
		Items []struct {
			ID        string    `json:"id"`
			Label     string    `json:"label"`
			Stream    string    `json:"stream"`
			Tags      []string  `json:"tags,omitempty"`
			CreatedAt time.Time `json:"created_at"`
		} `json:"items"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	if len(out.Items) == 0 {
		fmt.Fprintln(os.Stderr, "(no drops)")
		return nil
	}
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tCREATED\tSTREAM\tLABEL")
	for _, it := range out.Items {
		label := it.Label
		if len(label) > 60 {
			label = label[:60] + "…"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n",
			it.ID,
			it.CreatedAt.Local().Format("2006-01-02 15:04"),
			it.Stream, label)
	}
	return tw.Flush()
}
