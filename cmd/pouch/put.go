package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"
)

// runPut implements `pouch put` — read a body from FILE / stdin /
// clipboard, send it to the pouch ingress endpoint as a drop.
func runPut(args []string) error {
	fs := flag.NewFlagSet("put", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, "pouch put — send a drop to your pouch.\n\n"+
			"Usage:\n"+
			"  pouch put [FILE|-] [flags]\n\n"+
			"Body sources (one of):\n"+
			"  FILE                      read from the named file\n"+
			"  -                         read from stdin (also implicit if stdin is piped)\n"+
			"  --clipboard / -c          read from the system clipboard\n\n"+
			"Drop metadata:\n"+
			"  --label NAME              label (default: filename, or first line of body)\n"+
			"  --tag T                   add a tag (repeat for multiple)\n"+
			"  --tags A,B,C              add comma-separated tags\n"+
			"  --stream NAME             stream — usually 'inbox' (default) or 'kept'\n"+
			"  --mime TYPE               content-type hint (auto-detected from filename)\n"+
			"  --ttl 7d                  auto-delete after duration (e.g. 7d, 1h)\n"+
			"  --source NAME             source label (default: 'cli')\n\n"+
			"Binary:\n"+
			"  --binary                  allow non-UTF-8 / NUL-containing input. Without\n"+
			"                            this, pouch refuses to ingest binary so an\n"+
			"                            accidental 'cat image.jpg | pouch put' doesn't\n"+
			"                            land mojibake in your archive. Use with --mime.\n\n"+
			"Output:\n"+
			"  Prints the new drop's id on success.\n")
	}
	var (
		label     = fs.String("label", "", "")
		stream    = fs.String("stream", "inbox", "")
		tags      = fs.String("tags", "", "")
		tagFlag   stringSlice
		mimeType  = fs.String("mime", "", "")
		source    = fs.String("source", "cli", "")
		ttl       = fs.String("ttl", "", "")
		binaryOK  = fs.Bool("binary", false, "")
		clipboard = fs.Bool("clipboard", false, "")
		clipShort = fs.Bool("c", false, "")
		cfgPath   = fs.String("config", os.Getenv("POUCH_CONFIG"), "")
	)
	fs.Var(&tagFlag, "tag", "")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := loadCLIConfig(*cfgPath)
	if err != nil {
		return err
	}
	if cfg.URL == "" {
		return errors.New("POUCH_URL is required (set in env, --config, or ~/.config/pouch/config.env)")
	}
	if cfg.Key == "" {
		return errors.New("POUCH_KEY is required (ingress key from `pouch key create` on your pouch server)")
	}

	// Resolve body source. Precedence: --clipboard > FILE arg > - > piped stdin.
	var body []byte
	var defaultLabel string
	useClipboard := *clipboard || *clipShort

	switch {
	case useClipboard:
		body, err = readClipboard()
		if err != nil {
			return fmt.Errorf("clipboard: %w", err)
		}
		defaultLabel = "clipboard"

	case fs.NArg() > 0 && fs.Arg(0) != "-":
		path := fs.Arg(0)
		body, err = os.ReadFile(path)
		if err != nil {
			return err
		}
		defaultLabel = filepath.Base(path)
		if *mimeType == "" {
			*mimeType = mimeFromExt(filepath.Ext(path))
		}

	case fs.NArg() > 0 && fs.Arg(0) == "-":
		body, err = io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		defaultLabel = "stdin"

	default:
		// No FILE arg, no -, no --clipboard: only valid if stdin is piped.
		if !isStdinPiped() {
			return errors.New("nothing to send — pass FILE, -, --clipboard, or pipe stdin\nrun `pouch put --help` for usage")
		}
		body, err = io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		defaultLabel = "stdin"
	}

	if len(body) == 0 {
		return errors.New("empty body")
	}

	// Binary guard: if the bytes don't look like text (invalid UTF-8
	// or contain NUL), refuse unless --binary.
	if !looksLikeText(body) && !*binaryOK {
		return errors.New(
			"input looks binary (invalid UTF-8 or contains NUL bytes).\n" +
				"  - if intentional: pass --binary --mime <type> (e.g. --mime image/png)\n" +
				"  - note: pouch's binary-roundtrip support is provisional until binary-body-support ships,\n" +
				"    so re-reading the drop body may not preserve bytes exactly")
	}
	if *binaryOK && *mimeType == "" {
		return errors.New("--binary requires --mime (e.g. --mime image/png, --mime audio/wav, --mime application/pdf)")
	}

	// Default label: filename, or stripped first line of body.
	if *label == "" {
		*label = firstNonEmptyLine(body, defaultLabel, 80)
	}

	// Build query string.
	q := url.Values{}
	q.Set("label", *label)
	if *stream != "" {
		q.Set("stream", *stream)
	}
	if *source != "" {
		q.Set("source", *source)
	}
	if *ttl != "" {
		q.Set("ttl", *ttl)
	}

	// Tags: --tags A,B,C plus repeated --tag T. Dedup via set.
	tagSet := map[string]struct{}{}
	for _, t := range strings.Split(*tags, ",") {
		if t = strings.TrimSpace(t); t != "" {
			tagSet[t] = struct{}{}
		}
	}
	for _, t := range tagFlag {
		if t = strings.TrimSpace(t); t != "" {
			tagSet[t] = struct{}{}
		}
	}
	if len(tagSet) > 0 {
		out := make([]string, 0, len(tagSet))
		for t := range tagSet {
			out = append(out, t)
		}
		q.Set("tag", strings.Join(out, ","))
	}

	// POST to /ingress/<key> with raw body bytes. The endpoint reads
	// metadata from the query string + Content-Type header.
	u := strings.TrimRight(cfg.URL, "/") + "/ingress/" + cfg.Key + "?" + q.Encode()
	req, err := http.NewRequest("POST", u, bytes.NewReader(body))
	if err != nil {
		return err
	}
	if *mimeType != "" {
		req.Header.Set("Content-Type", *mimeType)
	} else {
		req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	}
	req.Header.Set("User-Agent", "pouch-cli/"+Version)

	cl := &http.Client{Timeout: 30 * time.Second}
	resp, err := cl.Do(req)
	if err != nil {
		return fmt.Errorf("post: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		buf, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("%d: %s", resp.StatusCode, strings.TrimSpace(string(buf)))
	}
	var out struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	fmt.Println(out.ID)
	return nil
}

// stringSlice is a flag.Value for repeated string flags (--tag a --tag b).
type stringSlice []string

func (s *stringSlice) String() string     { return strings.Join(*s, ",") }
func (s *stringSlice) Set(v string) error { *s = append(*s, v); return nil }

// looksLikeText is a heuristic: valid UTF-8 AND no NUL bytes. Most
// text files (any encoding that's UTF-8 compatible) pass; most
// binary files (image, audio, pdf, archive) fail. Cheap and good
// enough to catch the common foot-gun.
func looksLikeText(b []byte) bool {
	if !utf8.Valid(b) {
		return false
	}
	if bytes.IndexByte(b, 0) >= 0 {
		return false
	}
	return true
}

// firstNonEmptyLine returns the first non-empty trimmed line of b,
// capped at maxLen, or fallback if every line is blank.
func firstNonEmptyLine(b []byte, fallback string, maxLen int) string {
	for _, line := range strings.SplitN(string(b), "\n", 8) {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if utf8.RuneCountInString(line) > maxLen {
			// Trim by runes, not bytes, to avoid breaking codepoints.
			r := []rune(line)
			line = string(r[:maxLen]) + "…"
		}
		return line
	}
	return fallback
}

// mimeFromExt returns a sensible mime type for common extensions, or
// "" if unknown. Falls through to mime.TypeByExtension for the long
// tail. Filtered to a safe subset to avoid spelling out things like
// `application/octet-stream` for unknown extensions.
func mimeFromExt(ext string) string {
	ext = strings.ToLower(ext)
	switch ext {
	case ".md", ".markdown":
		return "text/markdown; charset=utf-8"
	case ".txt":
		return "text/plain; charset=utf-8"
	case ".json":
		return "application/json"
	case ".yaml", ".yml":
		return "application/yaml"
	case ".html", ".htm":
		return "text/html; charset=utf-8"
	}
	if t := mime.TypeByExtension(ext); t != "" {
		return t
	}
	return ""
}
