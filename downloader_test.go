package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDownloadState_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "downloads.json")

	st, err := loadDownloadState(path)
	if err != nil {
		t.Fatalf("load missing: %v", err)
	}
	if st == nil || len(st.Downloads) != 0 {
		t.Errorf("missing file should yield empty; got %+v", st)
	}

	now := time.Now().Truncate(time.Second)
	st.set(&downloadEntry{
		DropID:          "drop1",
		BlobID:          "blob1",
		DeliveryID:      "del1",
		SignedURL:       "https://cloud/api/blobs/blob1?sig=...",
		ExpectedSHA:     "abc123",
		Size:            10240,
		ReceivedThrough: 4096,
		Stream:          "scrapes",
		StreamLayout:    "mirror",
		MIME:            "image/png",
		Label:           "photo.png",
		OriginalPath:    "photos/2026/photo.png",
		QueuedAt:        now,
		LastProgressAt:  now,
	})
	if err := saveDownloadState(path, st); err != nil {
		t.Fatalf("save: %v", err)
	}
	st2, err := loadDownloadState(path)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	got := st2.Downloads["drop1"]
	if got == nil {
		t.Fatal("entry missing after reload")
	}
	if got.ReceivedThrough != 4096 || got.ExpectedSHA != "abc123" || got.StreamLayout != "mirror" {
		t.Errorf("entry corrupted on reload: %+v", got)
	}
}

func TestDownloadEntry_ToDropPreservesFields(t *testing.T) {
	e := &downloadEntry{
		DropID:       "d1",
		DeliveryID:   "del-1",
		PouchUser:    "u1",
		Stream:       "scrapes",
		Label:        "p.png",
		Tags:         []string{"a", "b"},
		MIME:         "image/png",
		Source:       "vault:k",
		OriginalPath: "x/y.png",
		CreatedAt:    time.Now().UTC(),
	}
	d := e.toDrop()
	if d.DropID != "d1" || d.PouchUser != "u1" || d.Stream != "scrapes" {
		t.Errorf("toDrop missing core fields: %+v", d)
	}
	if len(d.Tags) != 2 || d.OriginalPath != "x/y.png" {
		t.Errorf("toDrop missing metadata: %+v", d)
	}
}

func TestDownloadState_CorruptFileSurfacesError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "downloads.json")
	if err := os.WriteFile(path, []byte("not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadDownloadState(path); err == nil {
		t.Error("expected parse error on corrupt state")
	}
}

func TestVerifyFileSHA(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f.bin")
	if err := os.WriteFile(path, []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	// sha256("hello") = 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824
	if err := verifyFileSHA(path, "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"); err != nil {
		t.Errorf("hash match should pass: %v", err)
	}
	if err := verifyFileSHA(path, "deadbeef"); err == nil {
		t.Error("hash mismatch should fail")
	}
}
