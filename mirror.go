// mirror.go: vault's path-mirrored view. Phase 5 slice 8d.
//
// When a stream is configured with layout=mirror (cloud side), the
// vault writes each drop's bytes to mirror/<original_path> in
// addition to the canonical blobs/<sha> store. Latest-overwrite
// semantics — old versions stay in blobs/ and the catalog. The
// mirror is a derived "browse with native tools" view, not a
// history store.
//
// Drops with empty original_path skip the mirror write entirely
// even on a mirror-layout stream. Path-meaningless captures
// (clipboard, screenshots, voice clips) live only in blobs/ + DB.
package main

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// materializeMirror writes drop bytes to mirror/<original_path>
// when layout=="mirror" AND original_path is non-empty AND safe.
// Idempotent overwrite via temp + rename. Caller logs the returned
// error and continues — a single bad mirror write shouldn't sink
// the receive loop.
func materializeMirror(d *Drop, layout, blobsDir, mirrorDir string) error {
	if layout != "mirror" || d.OriginalPath == "" {
		return nil
	}
	rel, err := safeMirrorRel(d.OriginalPath)
	if err != nil {
		return err
	}
	bytes, err := loadDropBytes(d, blobsDir)
	if err != nil {
		return err
	}
	abs := filepath.Join(mirrorDir, rel)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return fmt.Errorf("mkdir mirror dir: %w", err)
	}
	tmp := abs + ".tmp"
	if err := os.WriteFile(tmp, bytes, 0o644); err != nil {
		return fmt.Errorf("write mirror: %w", err)
	}
	if err := os.Rename(tmp, abs); err != nil {
		return fmt.Errorf("rename mirror: %w", err)
	}
	return nil
}

// safeMirrorRel cleans + validates a producer-supplied path. Rejects
// absolute paths, traversal, and Windows-style backslashes. The
// vault may run on any filesystem; original_path was stamped on a
// possibly-different machine, so we treat it as untrusted input.
func safeMirrorRel(orig string) (string, error) {
	if orig == "" {
		return "", fmt.Errorf("empty path")
	}
	if strings.ContainsAny(orig, "\x00") {
		return "", fmt.Errorf("mirror: null byte in path %q", orig)
	}
	if strings.HasPrefix(orig, "/") || strings.Contains(orig, `\`) {
		return "", fmt.Errorf("mirror: rejecting non-relative path %q", orig)
	}
	clean := filepath.Clean(orig)
	if clean == "." || clean == "/" {
		return "", fmt.Errorf("mirror: rejecting empty-clean path %q", orig)
	}
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("mirror: rejecting traversal path %q", orig)
	}
	return clean, nil
}

// loadDropBytes returns the drop's raw bytes regardless of encoding.
// For blob-encoded drops the bytes are read from disk (materializeBlob
// has already spilled them); for base64 the body is decoded; for utf8
// the body is used as-is.
func loadDropBytes(d *Drop, blobsDir string) ([]byte, error) {
	switch d.BodyEncoding {
	case "blob":
		if d.BodyBlobPath == "" {
			return nil, fmt.Errorf("blob drop %s has no body_blob_path", d.DropID)
		}
		return os.ReadFile(filepath.Join(blobsDir, d.BodyBlobPath))
	case "base64":
		return base64.StdEncoding.DecodeString(d.Body)
	default: // utf8 or empty
		return []byte(d.Body), nil
	}
}
