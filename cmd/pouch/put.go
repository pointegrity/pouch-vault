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
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// inlineUploadCap is the threshold above which `pouch put` switches
// from the simple /ingress/{key} path to the blob path (POST /api/blobs
// then POST /api/items). Matches pouch's MaxBodyBytes default minus
// some slack for headers + JSON overhead.
const inlineUploadCap = 1 << 20

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
	// or contain NUL), require --binary so an accidental
	// `cat image.jpg | pouch put` doesn't sneak through with a
	// confusing default label.
	if !looksLikeText(body) && !*binaryOK {
		return errors.New(
			"input looks binary (invalid UTF-8 or contains NUL bytes).\n" +
				"  - if intentional: pass --binary --mime <type> (e.g. --mime image/png)\n" +
				"  - inline binary cap is 1 MB raw — pouch will base64-encode for you")
	}
	if *binaryOK && *mimeType == "" {
		return errors.New("--binary requires --mime (e.g. --mime image/png, --mime audio/wav, --mime application/pdf)")
	}

	// Default label: filename, or stripped first line of body.
	if *label == "" {
		*label = firstNonEmptyLine(body, defaultLabel, 80)
	}

	// Auto-route by size. Files over the inline cap go through the
	// blob path (POST /api/blobs + POST /api/items, both token-auth).
	// Smaller stays on the simple ingress key path.
	if len(body) > inlineUploadCap {
		return putViaBlobPath(cfg, body, *label, *mimeType, *stream, *source, *ttl,
			collectTags(*tags, tagFlag))
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

	// Tags via shared helper.
	tagList := collectTags(*tags, tagFlag)
	if len(tagList) > 0 {
		q.Set("tag", strings.Join(tagList, ","))
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

// putViaBlobPath uploads the body as a blob and creates an item that
// references it. Used for drops over inlineUploadCap. Requires the
// user to be logged in — the blob endpoints are token-auth only.
//
// Two HTTP calls:
//
//	POST /api/blobs               — body bytes; returns {id, sha256, size}
//	POST /api/items               — JSON {label, body_blob_id, mime, ...}
//
// Prints the new item id on success, same as the inline path.
func putViaBlobPath(cfg *CLIConfig, body []byte, label, mimeType, stream, source, ttl string, tags []string) error {
	tok, err := loadToken()
	if err != nil {
		return err
	}
	if tok == "" {
		return errors.New("file is over 1 MB — large drops use the blob path which requires `pouch login` first")
	}
	if mimeType == "" {
		// Multipart-style: detect from extension if filename hint exists,
		// else fall back to octet-stream so server stores something.
		mimeType = "application/octet-stream"
	}

	cl := &http.Client{Timeout: 5 * time.Minute}

	// 1. Upload bytes.
	blobURL := strings.TrimRight(cfg.URL, "/") + "/api/blobs"
	req, err := http.NewRequest("POST", blobURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", mimeType)
	req.Header.Set("Content-Length", strconv.Itoa(len(body)))
	req.Header.Set("User-Agent", "pouch-cli/"+Version)

	resp, err := cl.Do(req)
	if err != nil {
		return fmt.Errorf("blob upload: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		buf, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("blob upload %d: %s", resp.StatusCode, strings.TrimSpace(string(buf)))
	}
	var blob struct {
		ID     string `json:"id"`
		SHA256 string `json:"sha256"`
		Size   int64  `json:"size"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&blob); err != nil {
		return fmt.Errorf("decode blob response: %w", err)
	}

	// 2. Create item referencing the blob.
	itemPayload := map[string]any{
		"label":         label,
		"body_blob_id":  blob.ID,
		"body_encoding": "blob",
		"mime":          mimeType,
		"source":        source,
	}
	if stream != "" {
		itemPayload["stream"] = stream
	}
	if len(tags) > 0 {
		itemPayload["tags"] = tags
	}
	if ttl != "" {
		if d, err := time.ParseDuration(ttl); err == nil {
			itemPayload["ttl_at"] = time.Now().Add(d).UTC().Format(time.RFC3339)
		}
	}
	itemBody, _ := json.Marshal(itemPayload)
	itemURL := strings.TrimRight(cfg.URL, "/") + "/api/items"
	req2, _ := http.NewRequest("POST", itemURL, bytes.NewReader(itemBody))
	req2.Header.Set("Authorization", "Bearer "+tok)
	req2.Header.Set("Content-Type", "application/json")
	req2.Header.Set("User-Agent", "pouch-cli/"+Version)

	resp2, err := cl.Do(req2)
	if err != nil {
		return fmt.Errorf("create item: %w", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode >= 400 {
		buf, _ := io.ReadAll(io.LimitReader(resp2.Body, 1024))
		return fmt.Errorf("create item %d: %s", resp2.StatusCode, strings.TrimSpace(string(buf)))
	}
	var out struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp2.Body).Decode(&out); err != nil {
		return fmt.Errorf("decode item response: %w", err)
	}
	fmt.Fprintf(os.Stderr, "uploaded %s (%s, %d bytes) as blob %s\n",
		label, mimeType, blob.Size, blob.ID)
	fmt.Println(out.ID)
	return nil
}

// collectTags merges --tags=A,B,C with repeated --tag T flags into
// a deduped list. Order is not stable (map iteration); doesn't matter
// for tags semantically.
func collectTags(comma string, repeated stringSlice) []string {
	set := map[string]struct{}{}
	for _, t := range strings.Split(comma, ",") {
		if t = strings.TrimSpace(t); t != "" {
			set[t] = struct{}{}
		}
	}
	for _, t := range repeated {
		if t = strings.TrimSpace(t); t != "" {
			set[t] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for t := range set {
		out = append(out, t)
	}
	return out
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
