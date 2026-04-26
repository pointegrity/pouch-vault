// blobs.go: anchor's local blob storage. Binary drops over the
// inline threshold get base64-decoded and written to disk so users
// can browse them with native tools (`find . -name '*.png'`,
// Preview, mpv, …) instead of having a base64 string sitting
// uselessly in SQLite.
//
// Layout under blobsDir:
//
//	<aa>/<sha256><ext>
//
// where <aa> is the first two hex chars of sha256 (sharded for
// filesystem performance) and <ext> is derived from MIME, e.g. .png,
// .jpg, .pdf — purely cosmetic so `ls` shows recognizable names.
// The bytes are content-addressed; identical drops uploaded twice
// dedup naturally.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// httpNewRequestWithContext is a thin alias so we can keep the
// blob client + helpers in this file without importing net/http
// twice.
var httpNewRequestWithContext = http.NewRequestWithContext

// blobHTTPClient has a generous timeout — large blobs over slow
// links should still complete. 5 minutes (matches the signed-URL
// validity window).
var blobHTTPClient = &http.Client{Timeout: 5 * time.Minute}

// readAllN reads up to limit+1 bytes from r so callers can detect
// "bigger than expected" without OOMing.
func readAllN(r io.Reader, limit int64) ([]byte, error) {
	if limit < 0 {
		limit = 50 << 20 // 50 MiB hard cap defensive
	}
	buf := make([]byte, 0, 1024)
	tmp := make([]byte, 64*1024)
	for {
		n, err := r.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
			if int64(len(buf)) > limit {
				return buf, nil
			}
		}
		if err == io.EOF {
			return buf, nil
		}
		if err != nil {
			return buf, err
		}
	}
}

// inlineThreshold is the byte size up-to-which a binary drop's
// base64 stays in SQLite directly. Above this we materialize.
//
// Tuning: keep small drops in DB so SELECT-then-render works
// without a separate fetch; spill larger drops to disk so SQLite
// doesn't bloat. 64 KiB raw → ~85 KiB base64 in DB; works well
// for tiny screenshots, voice clips, etc.
const inlineThreshold = 64 * 1024

// materializeBlobs decodes the drop's body if it's base64-encoded
// AND large enough to spill, writing the bytes to blobsDir and
// updating the Drop fields in place. No-op for utf8 drops or for
// inline-small base64 drops.
func materializeBlob(d *Drop, blobsDir string) error {
	if d.BodyEncoding != "base64" {
		return nil
	}
	// Decoded size = 3/4 of base64 length (approx). We check
	// against threshold using the BASE64 length to avoid decoding
	// in the hot path for small drops.
	if len(d.Body) < (inlineThreshold*4)/3 {
		return nil
	}
	decoded, err := base64.StdEncoding.DecodeString(d.Body)
	if err != nil {
		return fmt.Errorf("decode base64 body: %w", err)
	}
	sum := sha256.Sum256(decoded)
	sha := hex.EncodeToString(sum[:])
	ext := blobExtFromMIME(d.MIME)
	rel := filepath.Join(sha[:2], sha+ext)
	abs := filepath.Join(blobsDir, rel)

	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return fmt.Errorf("mkdir blob dir: %w", err)
	}
	// Write atomically via temp + rename so a crash mid-write
	// doesn't leave a half-blob with the canonical name.
	tmp := abs + ".tmp"
	if err := os.WriteFile(tmp, decoded, 0o644); err != nil {
		return fmt.Errorf("write blob: %w", err)
	}
	if err := os.Rename(tmp, abs); err != nil {
		return fmt.Errorf("rename blob: %w", err)
	}

	d.BodySHA256 = sha
	d.BodyBlobPath = rel
	d.BodySize = int64(len(decoded))
	d.Body = "" // free up DB space — canonical bytes are on disk now
	d.BodyEncoding = "blob"
	return nil
}

// fetchBlobToDisk pulls a blob from a signed URL pouch handed us
// over the SSE wire, verifies the sha256, and writes to blobsDir.
// Returns the relative on-disk path on success.
//
// Used by the SSE handler when an incoming drop carries a Blob
// manifest instead of an inline body.
func fetchBlobToDisk(ctx context.Context, blobsDir, signedURL, expectedSHA256, mime string, expectedSize int64) (string, error) {
	req, err := httpNewRequestWithContext(ctx, "GET", signedURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "pouch-anchor/"+Version)
	resp, err := blobHTTPClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("fetch blob: %s", resp.Status)
	}
	body, err := readAllN(resp.Body, expectedSize+1024) // small slack
	if err != nil {
		return "", err
	}
	if int64(len(body)) > expectedSize+1024 {
		return "", errors.New("blob bigger than manifest claimed")
	}
	sum := sha256.Sum256(body)
	got := hex.EncodeToString(sum[:])
	if expectedSHA256 != "" && got != expectedSHA256 {
		return "", fmt.Errorf("sha256 mismatch: got %s, want %s", got, expectedSHA256)
	}
	ext := blobExtFromMIME(mime)
	rel := filepath.Join(got[:2], got+ext)
	abs := filepath.Join(blobsDir, rel)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return "", err
	}
	tmp := abs + ".tmp"
	if err := os.WriteFile(tmp, body, 0o644); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, abs); err != nil {
		return "", err
	}
	return rel, nil
}

// readBlob returns the bytes for a stored blob keyed on sha256.
// Walks the sharded directory to find the file (extension is
// cosmetic, so we glob).
func readBlob(blobsDir, sha string) ([]byte, string, error) {
	if len(sha) < 2 {
		return nil, "", errors.New("invalid sha")
	}
	matches, err := filepath.Glob(filepath.Join(blobsDir, sha[:2], sha+"*"))
	if err != nil {
		return nil, "", err
	}
	if len(matches) == 0 {
		return nil, "", os.ErrNotExist
	}
	b, err := os.ReadFile(matches[0])
	if err != nil {
		return nil, "", err
	}
	return b, matches[0], nil
}

// blobExtFromMIME returns a short, conventional file extension for
// a given content-type. Empty string when unknown — the file still
// gets written, just without an extension.
func blobExtFromMIME(mime string) string {
	mime = strings.TrimSpace(strings.ToLower(mime))
	if i := strings.IndexByte(mime, ';'); i >= 0 {
		mime = strings.TrimSpace(mime[:i])
	}
	switch mime {
	case "image/png":
		return ".png"
	case "image/jpeg":
		return ".jpg"
	case "image/gif":
		return ".gif"
	case "image/webp":
		return ".webp"
	case "image/heic":
		return ".heic"
	case "image/svg+xml":
		return ".svg"
	case "audio/mpeg":
		return ".mp3"
	case "audio/wav", "audio/x-wav":
		return ".wav"
	case "audio/mp4", "audio/m4a":
		return ".m4a"
	case "audio/ogg":
		return ".ogg"
	case "video/mp4":
		return ".mp4"
	case "video/quicktime":
		return ".mov"
	case "video/webm":
		return ".webm"
	case "application/pdf":
		return ".pdf"
	case "application/zip":
		return ".zip"
	}
	return ""
}
