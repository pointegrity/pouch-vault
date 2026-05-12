package main

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSafeMirrorRel(t *testing.T) {
	cases := []struct {
		in       string
		wantErr  bool
		wantPath string
	}{
		{"foo.txt", false, "foo.txt"},
		{"a/b/c.txt", false, "a/b/c.txt"},
		{"scrapes/example.com/page.html", false, "scrapes/example.com/page.html"},
		// filepath.Clean normalizes
		{"./foo.txt", false, "foo.txt"},
		{"foo//bar.txt", false, "foo/bar.txt"},
		// rejections
		{"", true, ""},
		{"/etc/passwd", true, ""},
		{"..", true, ""},
		{"../foo.txt", true, ""},
		{`a\b\c.txt`, true, ""},
		{"foo\x00bar", true, ""},
	}
	for _, tc := range cases {
		got, err := safeMirrorRel(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Errorf("safeMirrorRel(%q) = %q; want error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("safeMirrorRel(%q) error: %v", tc.in, err)
			continue
		}
		if got != tc.wantPath {
			t.Errorf("safeMirrorRel(%q) = %q; want %q", tc.in, got, tc.wantPath)
		}
	}
}

func TestMaterializeMirror_FlatLayoutIsNoop(t *testing.T) {
	dir := t.TempDir()
	mirrorDir := filepath.Join(dir, "mirror")
	d := &Drop{
		DropID:       "drop-1",
		OriginalPath: "foo/bar.txt",
		Body:         "hello",
		BodyEncoding: "utf8",
	}
	if err := materializeMirror(d, "flat", dir, mirrorDir); err != nil {
		t.Fatalf("flat: %v", err)
	}
	if _, err := os.Stat(mirrorDir); !os.IsNotExist(err) {
		t.Errorf("flat layout should not create mirror/; got err=%v", err)
	}
}

func TestMaterializeMirror_EmptyOriginalPathIsNoop(t *testing.T) {
	dir := t.TempDir()
	mirrorDir := filepath.Join(dir, "mirror")
	d := &Drop{
		DropID:       "drop-1",
		OriginalPath: "",
		Body:         "hello",
		BodyEncoding: "utf8",
	}
	if err := materializeMirror(d, "mirror", dir, mirrorDir); err != nil {
		t.Fatalf("empty path: %v", err)
	}
	if _, err := os.Stat(mirrorDir); !os.IsNotExist(err) {
		t.Errorf("empty original_path on mirror stream should not write; got err=%v", err)
	}
}

func TestMaterializeMirror_UTF8Body(t *testing.T) {
	dir := t.TempDir()
	mirrorDir := filepath.Join(dir, "mirror")
	d := &Drop{
		DropID:       "drop-utf8",
		OriginalPath: "notes/hello.md",
		Body:         "# hello world",
		BodyEncoding: "utf8",
	}
	if err := materializeMirror(d, "mirror", dir, mirrorDir); err != nil {
		t.Fatalf("utf8: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(mirrorDir, "notes", "hello.md"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "# hello world" {
		t.Errorf("got %q; want %q", got, "# hello world")
	}
}

func TestMaterializeMirror_Base64Body(t *testing.T) {
	dir := t.TempDir()
	mirrorDir := filepath.Join(dir, "mirror")
	raw := []byte{0x89, 0x50, 0x4E, 0x47, 0x0d, 0x0a} // fake PNG header
	d := &Drop{
		DropID:       "drop-png",
		OriginalPath: "photos/2026/img.png",
		Body:         base64.StdEncoding.EncodeToString(raw),
		BodyEncoding: "base64",
	}
	if err := materializeMirror(d, "mirror", dir, mirrorDir); err != nil {
		t.Fatalf("base64: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(mirrorDir, "photos/2026/img.png"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != string(raw) {
		t.Errorf("bytes mismatch: %x vs %x", got, raw)
	}
}

func TestMaterializeMirror_BlobBody(t *testing.T) {
	dir := t.TempDir()
	mirrorDir := filepath.Join(dir, "mirror")
	blobsDir := filepath.Join(dir, "blobs")
	relBlob := filepath.Join("ab", "abcdef.bin")
	absBlob := filepath.Join(blobsDir, relBlob)
	if err := os.MkdirAll(filepath.Dir(absBlob), 0o755); err != nil {
		t.Fatal(err)
	}
	want := []byte("blob bytes here")
	if err := os.WriteFile(absBlob, want, 0o644); err != nil {
		t.Fatal(err)
	}
	d := &Drop{
		DropID:       "drop-blob",
		OriginalPath: "docs/large.bin",
		BodyEncoding: "blob",
		BodyBlobPath: relBlob,
		BodySHA256:   "abcdef",
	}
	if err := materializeMirror(d, "mirror", blobsDir, mirrorDir); err != nil {
		t.Fatalf("blob: %v", err)
	}
	got, err := os.ReadFile(filepath.Join(mirrorDir, "docs/large.bin"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("bytes mismatch: %q vs %q", got, want)
	}
}

func TestMaterializeMirror_LatestOverwrite(t *testing.T) {
	dir := t.TempDir()
	mirrorDir := filepath.Join(dir, "mirror")
	d1 := &Drop{DropID: "v1", OriginalPath: "f.txt", Body: "v1", BodyEncoding: "utf8"}
	d2 := &Drop{DropID: "v2", OriginalPath: "f.txt", Body: "v2", BodyEncoding: "utf8"}
	if err := materializeMirror(d1, "mirror", dir, mirrorDir); err != nil {
		t.Fatal(err)
	}
	if err := materializeMirror(d2, "mirror", dir, mirrorDir); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(filepath.Join(mirrorDir, "f.txt"))
	if string(got) != "v2" {
		t.Errorf("latest-overwrite: got %q; want v2", got)
	}
}

func TestMaterializeMirror_TraversalRejected(t *testing.T) {
	dir := t.TempDir()
	mirrorDir := filepath.Join(dir, "mirror")
	d := &Drop{
		DropID:       "evil",
		OriginalPath: "../etc/passwd",
		Body:         "evil",
		BodyEncoding: "utf8",
	}
	err := materializeMirror(d, "mirror", dir, mirrorDir)
	if err == nil {
		t.Fatal("expected rejection")
	}
	if !strings.Contains(err.Error(), "traversal") && !strings.Contains(err.Error(), "non-relative") {
		t.Errorf("error message should hint at traversal: %v", err)
	}
	// Confirm nothing escaped.
	if _, err := os.Stat(filepath.Join(filepath.Dir(mirrorDir), "etc/passwd")); !os.IsNotExist(err) {
		t.Errorf("traversal write happened; got err=%v", err)
	}
}
