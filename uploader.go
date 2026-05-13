// pouch-vault chunked-upload producer (Phase 5 slice 8e.producer).
//
// Handles files over the inline-cap threshold by driving the
// resumable PATCH-by-range protocol against /api/blobs/*. Single-
// link discipline: one chunk in flight across the whole vault
// daemon. Throttled (default 250ms inter-chunk sleep) to stay
// well under the cloud's per-user cap and to leave the user's
// network bandwidth alone — this is meant to be a slow background
// process.
//
// State file at <state-dir>/transfers.json records every in-flight
// transfer; on producer restart, each entry is consulted against
// /api/blobs/{id}/status to resume from the last received offset.
//
// Wire decision: stream-storage-and-chunked-transfer.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// newUploaderFromConfig constructs the uploader using env vars
// VAULT_UPLOAD_CHUNK (MiB) and VAULT_UPLOAD_THROTTLE_MS, falling
// back to UploadChunkSize / UploadThrottleMS. State file lives
// under the OS-conventional state dir for pouch-vault, alongside
// sync.json.
func newUploaderFromConfig(client *PouchClient, _ *config) *Uploader {
	chunk := int64(UploadChunkSize)
	if s := os.Getenv("VAULT_UPLOAD_CHUNK"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			chunk = int64(n) * 1024 * 1024
		}
	}
	throttle := time.Duration(UploadThrottleMS) * time.Millisecond
	if s := os.Getenv("VAULT_UPLOAD_THROTTLE_MS"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n >= 0 {
			throttle = time.Duration(n) * time.Millisecond
		}
	}
	statePath := uploaderStatePath()
	return NewUploader(client, statePath, chunk, throttle)
}

// uploaderStatePath returns ~/.local/state/pouch-vault/transfers.json
// or the OS equivalent. Falls back to ./transfers.json when the home
// directory can't be resolved (rare).
func uploaderStatePath() string {
	if dir, err := os.UserHomeDir(); err == nil {
		return filepath.Join(dir, ".local", "state", "pouch-vault", "transfers.json")
	}
	return "transfers.json"
}

// UploadChunkSize is the default per-PATCH body size. 4 MiB is a
// compromise: small enough that a crash loses little, large enough
// that the round-trip cost is amortized. Override via
// VAULT_UPLOAD_CHUNK env (in MiB).
const UploadChunkSize = 4 * 1024 * 1024

// UploadThrottleMS is the default inter-PATCH sleep. 250 ms yields
// ~16 MiB/s with 4 MiB chunks — gentle on the network, leaves
// laptop CPU mostly idle. Override via VAULT_UPLOAD_THROTTLE_MS.
const UploadThrottleMS = 250

// UploadRetryDelays controls the backoff schedule for transient
// PATCH/Open errors. After exhausting these, the transfer halts
// and the state entry is marked with .Error so the operator can
// see why.
var UploadRetryDelays = []time.Duration{
	time.Second, 4 * time.Second, 16 * time.Second,
}

// Uploader owns the single-link mutex and the transfers.json state.
// One per process; created in main() and handed to sync/watch.
type Uploader struct {
	client    *PouchClient
	statePath string
	chunkSize int64
	throttle  time.Duration

	mu    sync.Mutex // process-wide single-link guard
	state *uploadState
}

// NewUploader constructs an uploader. statePath is typically
// ~/.local/state/pouch-vault/transfers.json (resolved at the call
// site). Initial state load is best-effort: a corrupt or missing
// file becomes an empty in-memory state, which the next save will
// rewrite.
func NewUploader(client *PouchClient, statePath string, chunkSize int64, throttle time.Duration) *Uploader {
	if chunkSize <= 0 {
		chunkSize = UploadChunkSize
	}
	if throttle < 0 {
		throttle = UploadThrottleMS * time.Millisecond
	}
	st, err := loadUploadState(statePath)
	if err != nil {
		log.Printf("uploader: state load (%s): %v — starting fresh", statePath, err)
		st = newUploadState()
	}
	return &Uploader{
		client:    client,
		statePath: statePath,
		chunkSize: chunkSize,
		throttle:  throttle,
		state:     st,
	}
}

// UploadFile chunk-uploads absPath to cloud and returns the blob id.
// Single-link: serializes across goroutines. expectedSHA + size are
// used to dedupe with any matching entry in transfers.json (so
// re-runs after a producer crash resume the same id instead of
// starting fresh).
//
// On success: transfers.json entry is cleared. The caller follows
// up with POST /api/items referencing the returned blob_id.
//
// On halt (retries exhausted, sha mismatch, etc.): transfers.json
// entry retains Error so the operator can see what happened. The
// caller treats the file as failed-for-this-pass and moves on.
func (u *Uploader) UploadFile(ctx context.Context, absPath, mime, expectedSHA string, expectedSize int64) (blobID string, err error) {
	u.mu.Lock()
	defer u.mu.Unlock()

	entry := u.state.findMatching(absPath, expectedSHA, expectedSize)
	if entry != nil && entry.Error != "" {
		// A prior pass halted on this file. Clear the error so this
		// pass starts fresh — operator may have fixed network etc.
		entry.Error = ""
	}
	if entry == nil {
		open, err := withRetry(ctx, "open blob", func(ctx context.Context) (*BlobOpenResult, error) {
			return u.client.OpenBlob(ctx, mime)
		})
		if err != nil {
			return "", fmt.Errorf("open blob: %w", err)
		}
		entry = &uploadEntry{
			BlobID:         open.ID,
			AbsPath:        absPath,
			SHA256:         expectedSHA,
			Size:           expectedSize,
			OffsetThrough:  0,
			StartedAt:      time.Now(),
			LastProgressAt: time.Now(),
		}
		u.state.set(entry)
		if err := u.persist(); err != nil {
			return "", fmt.Errorf("persist state after open: %w", err)
		}
	} else {
		// Resume — confirm cloud agrees with our offset.
		st, err := u.client.GetBlobStatus(ctx, entry.BlobID)
		if err != nil {
			if errors.Is(err, ErrBlobNotFound) {
				// Cloud forgot. Start fresh.
				u.state.delete(entry.BlobID)
				return u.UploadFile(ctx, absPath, mime, expectedSHA, expectedSize)
			}
			return "", fmt.Errorf("status: %w", err)
		}
		if st.Status != "open" {
			u.state.delete(entry.BlobID)
			if st.Status == "ready" {
				// Cloud already has it; nothing to do.
				log.Printf("uploader: blob %s already ready on cloud", entry.BlobID)
				return entry.BlobID, u.persist()
			}
			// cancelled / relayed* — start fresh.
			return u.UploadFile(ctx, absPath, mime, expectedSHA, expectedSize)
		}
		if st.ReceivedThrough < entry.OffsetThrough {
			// Cloud has less than we thought — trust the cloud.
			entry.OffsetThrough = st.ReceivedThrough
		} else if st.ReceivedThrough > entry.OffsetThrough {
			// Cloud is ahead (we crashed mid-state-write?). Adopt
			// cloud's offset.
			entry.OffsetThrough = st.ReceivedThrough
		}
	}

	if err := u.streamChunks(ctx, entry); err != nil {
		entry.Error = err.Error()
		_ = u.persist()
		return "", err
	}

	// Verify our local sha matches what we'll send to /complete.
	if expectedSHA == "" {
		return "", fmt.Errorf("expected sha256 required")
	}
	if err := withRetryNoReturn(ctx, "complete blob", func(ctx context.Context) error {
		return u.client.CompleteBlob(ctx, entry.BlobID, expectedSHA, expectedSize)
	}); err != nil {
		entry.Error = "complete: " + err.Error()
		_ = u.persist()
		return "", fmt.Errorf("complete: %w", err)
	}

	u.state.delete(entry.BlobID)
	if err := u.persist(); err != nil {
		log.Printf("uploader: persist after complete: %v (continuing)", err)
	}
	return entry.BlobID, nil
}

// streamChunks reads from entry.OffsetThrough to entry.Size, PATCHing
// each chunk. Updates entry.OffsetThrough on every success and
// persists state. Errors here halt the transfer; caller records the
// error on the state entry.
func (u *Uploader) streamChunks(ctx context.Context, entry *uploadEntry) error {
	if entry.OffsetThrough >= entry.Size {
		return nil // already fully uploaded; just needs /complete
	}
	f, err := os.Open(entry.AbsPath)
	if err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	defer f.Close()
	if _, err := f.Seek(entry.OffsetThrough, io.SeekStart); err != nil {
		return fmt.Errorf("seek: %w", err)
	}
	buf := make([]byte, u.chunkSize)
	for entry.OffsetThrough < entry.Size {
		if err := ctx.Err(); err != nil {
			return err
		}
		want := u.chunkSize
		if remaining := entry.Size - entry.OffsetThrough; remaining < want {
			want = remaining
		}
		n, err := io.ReadFull(f, buf[:want])
		if err != nil && err != io.ErrUnexpectedEOF {
			return fmt.Errorf("read source: %w", err)
		}
		if int64(n) != want {
			return fmt.Errorf("short read: got %d, wanted %d", n, want)
		}
		startOff := entry.OffsetThrough
		endOff := startOff + int64(n) - 1
		res, err := withRetry(ctx, fmt.Sprintf("patch %d-%d", startOff, endOff), func(ctx context.Context) (*BlobPatchResult, error) {
			return u.client.PatchBlob(ctx, entry.BlobID, startOff, endOff, entry.Size, buf[:n])
		})
		if err != nil {
			return err
		}
		entry.OffsetThrough = res.ReceivedThrough
		entry.LastProgressAt = time.Now()
		if err := u.persist(); err != nil {
			return fmt.Errorf("persist progress: %w", err)
		}
		if u.throttle > 0 && entry.OffsetThrough < entry.Size {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(u.throttle):
			}
		}
	}
	return nil
}

// withRetry runs op with the configured backoff schedule. Returns
// the first successful result, or the last error after exhausting
// the retry budget.
func withRetry[T any](ctx context.Context, label string, op func(context.Context) (T, error)) (T, error) {
	var zero T
	var lastErr error
	for attempt, delay := range append([]time.Duration{0}, UploadRetryDelays...) {
		if delay > 0 {
			select {
			case <-ctx.Done():
				return zero, ctx.Err()
			case <-time.After(delay):
			}
		}
		out, err := op(ctx)
		if err == nil {
			return out, nil
		}
		lastErr = err
		// Cap-exceeded: longer sleep, retry as usual (cloud will
		// drain).
		if errors.Is(err, ErrCloudCapExceeded) {
			log.Printf("uploader: %s cap-exceeded; sleeping 30s", label)
			select {
			case <-ctx.Done():
				return zero, ctx.Err()
			case <-time.After(30 * time.Second):
			}
			continue
		}
		log.Printf("uploader: %s attempt %d failed: %v", label, attempt+1, err)
	}
	return zero, lastErr
}

func withRetryNoReturn(ctx context.Context, label string, op func(context.Context) error) error {
	_, err := withRetry(ctx, label, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, op(ctx)
	})
	return err
}

// FileSHA returns the sha256 of a file's contents, hex-encoded.
// Used by sync/watch before calling UploadFile so the uploader has
// a precomputed value to dedupe state entries.
func FileSHA(absPath string) (string, error) {
	f, err := os.Open(absPath)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// persist atomically writes the current in-memory state to disk.
// Caller holds u.mu.
func (u *Uploader) persist() error {
	return saveUploadState(u.statePath, u.state)
}

// --- transfers.json persistence ---

type uploadState struct {
	Transfers map[string]*uploadEntry `json:"transfers"`
}

type uploadEntry struct {
	BlobID         string    `json:"blob_id"`
	AbsPath        string    `json:"abs_path"`
	SHA256         string    `json:"sha256"`
	Size           int64     `json:"size"`
	OffsetThrough  int64     `json:"offset_through"`
	StartedAt      time.Time `json:"started_at"`
	LastProgressAt time.Time `json:"last_progress_at"`
	Error          string    `json:"error,omitempty"`
}

func newUploadState() *uploadState {
	return &uploadState{Transfers: map[string]*uploadEntry{}}
}

// findMatching looks for an existing entry whose (absPath, sha256,
// size) match. Returns nil if there's no match — caller opens a
// fresh blob id.
func (s *uploadState) findMatching(absPath, sha string, size int64) *uploadEntry {
	for _, e := range s.Transfers {
		if e.AbsPath == absPath && e.SHA256 == sha && e.Size == size {
			return e
		}
	}
	return nil
}

func (s *uploadState) set(e *uploadEntry) {
	s.Transfers[e.BlobID] = e
}

func (s *uploadState) delete(blobID string) {
	delete(s.Transfers, blobID)
}

func loadUploadState(path string) (*uploadState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return newUploadState(), nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return newUploadState(), nil
	}
	var st uploadState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, err
	}
	if st.Transfers == nil {
		st.Transfers = map[string]*uploadEntry{}
	}
	return &st, nil
}

func saveUploadState(path string, st *uploadState) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
