package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestUploadState_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "transfers.json")

	// Empty file → empty state.
	st, err := loadUploadState(statePath)
	if err != nil {
		t.Fatalf("load missing: %v", err)
	}
	if st == nil || len(st.Transfers) != 0 {
		t.Errorf("missing file should yield empty state; got %+v", st)
	}

	// Save → load round trip.
	now := time.Now().Truncate(time.Second)
	st.set(&uploadEntry{
		BlobID:         "blob-xyz",
		AbsPath:        "/a/b/file.bin",
		SHA256:         "abc123",
		Size:           4096,
		OffsetThrough:  2048,
		StartedAt:      now,
		LastProgressAt: now,
	})
	if err := saveUploadState(statePath, st); err != nil {
		t.Fatalf("save: %v", err)
	}
	st2, err := loadUploadState(statePath)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	got := st2.Transfers["blob-xyz"]
	if got == nil {
		t.Fatal("entry missing after reload")
	}
	if got.OffsetThrough != 2048 || got.SHA256 != "abc123" {
		t.Errorf("entry corrupted on reload: %+v", got)
	}
}

func TestUploadState_FindMatching(t *testing.T) {
	st := newUploadState()
	st.set(&uploadEntry{BlobID: "b1", AbsPath: "/a/x", SHA256: "sha-x", Size: 100})
	st.set(&uploadEntry{BlobID: "b2", AbsPath: "/a/y", SHA256: "sha-y", Size: 200})

	// Exact (path, sha, size) match
	if got := st.findMatching("/a/x", "sha-x", 100); got == nil || got.BlobID != "b1" {
		t.Errorf("findMatching(/a/x) = %v; want b1", got)
	}
	// Same path, different sha → no match
	if got := st.findMatching("/a/x", "sha-changed", 100); got != nil {
		t.Errorf("findMatching with changed sha should be nil; got %v", got)
	}
	// Same path + sha but size differs → no match
	if got := st.findMatching("/a/x", "sha-x", 101); got != nil {
		t.Errorf("findMatching with size diff should be nil; got %v", got)
	}
	// Unknown path → no match
	if got := st.findMatching("/a/z", "sha-z", 50); got != nil {
		t.Errorf("findMatching unknown should be nil; got %v", got)
	}
}

func TestUploadState_CorruptFileRecoversFresh(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "transfers.json")
	if err := os.WriteFile(statePath, []byte(`{not json`), 0o600); err != nil {
		t.Fatal(err)
	}
	// loadUploadState returns the parse error so NewUploader can
	// log + start fresh; the call here exercises that branch.
	_, err := loadUploadState(statePath)
	if err == nil {
		t.Error("expected unmarshal error on corrupt file")
	}
}
