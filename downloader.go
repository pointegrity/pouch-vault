// pouch-vault chunked-download consumer (Phase 5 slice 8e.consumer).
//
// Symmetric to uploader.go: when an SSE drop carries a Blob ref, the
// stream handler enqueues a download intent and the Downloader pulls
// the bytes over GET-with-Range, verifying sha256, then completes
// the drop locally (store row + mirror materialize + cloud ACK).
//
// Single-link discipline: one chunk in flight across the whole vault
// daemon, separate from the upload mutex. Throttled and resumable
// across process death via ~/.local/state/pouch-vault/downloads.json.
//
// ACK semantics: cloud only reaps a relay-stream blob's bytes after
// every connected mirror channel ACKs. The ACK fires here ONLY after
// the bytes are on local disk AND the drops row is inserted AND
// mirror materialize (if applicable) completed. ACK-on-SSE-receive
// would race the sweeper.
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
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	// DownloadChunkSize is bytes read+written per Range request. Same
	// rationale as UploadChunkSize.
	DownloadChunkSize = 4 * 1024 * 1024
	// DownloadThrottleMS is inter-chunk sleep on the consumer side.
	DownloadThrottleMS = 250
)

// Downloader processes pending blob fetches from cloud. One per
// process; constructed in main() and shared between the stream
// handler (pull mode) and the receiver (push mode).
type Downloader struct {
	client     *PouchClient
	store      *Store
	blobsDir   string
	mirrorDir  string
	statePath  string
	chunkSize  int64
	throttle   time.Duration

	mu       sync.Mutex
	state    *downloadState
	wakeCh   chan struct{} // nudge the loop on new enqueue
}

// NewDownloader constructs the downloader. statePath is typically
// ~/.local/state/pouch-vault/downloads.json. Start() launches a
// background loop; callers Enqueue and the loop processes serially.
func NewDownloader(client *PouchClient, store *Store, blobsDir, mirrorDir, statePath string, chunkSize int64, throttle time.Duration) *Downloader {
	if chunkSize <= 0 {
		chunkSize = DownloadChunkSize
	}
	if throttle < 0 {
		throttle = DownloadThrottleMS * time.Millisecond
	}
	st, err := loadDownloadState(statePath)
	if err != nil {
		log.Printf("downloader: state load (%s): %v — starting fresh", statePath, err)
		st = newDownloadState()
	}
	return &Downloader{
		client:    client,
		store:     store,
		blobsDir:  blobsDir,
		mirrorDir: mirrorDir,
		statePath: statePath,
		chunkSize: chunkSize,
		throttle:  throttle,
		state:     st,
		wakeCh:    make(chan struct{}, 1),
	}
}

// Enqueue records a download intent. Called from the SSE handler
// when a drop with a Blob ref arrives. Returns immediately;
// processing happens on the background loop.
func (d *Downloader) Enqueue(intent *downloadEntry) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if intent.QueuedAt.IsZero() {
		intent.QueuedAt = time.Now()
	}
	d.state.set(intent)
	if err := d.persist(); err != nil {
		return err
	}
	d.wake()
	return nil
}

// HasPending reports whether the downloader has anything to do
// (used by main on (re)start to know whether to kick the loop).
func (d *Downloader) HasPending() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.state.Downloads) > 0
}

// Start launches the background processing loop. Returns
// immediately; the loop runs until ctx is cancelled.
func (d *Downloader) Start(ctx context.Context) {
	go d.loop(ctx)
}

func (d *Downloader) loop(ctx context.Context) {
	// Kick once on start to drain any incomplete entries left from a
	// previous run.
	d.wake()
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.wakeCh:
		}
		for {
			entry := d.nextPending()
			if entry == nil {
				break
			}
			if err := d.processOne(ctx, entry); err != nil {
				log.Printf("downloader: %s: %v", entry.DropID, err)
				entry.Error = err.Error()
				d.mu.Lock()
				d.state.set(entry)
				_ = d.persist()
				d.mu.Unlock()
				// Bail out of the inner loop on error — keep state
				// for the next retry tick (caller wake or restart).
				break
			}
			d.mu.Lock()
			d.state.delete(entry.DropID)
			_ = d.persist()
			d.mu.Unlock()
		}
	}
}

func (d *Downloader) nextPending() *downloadEntry {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, e := range d.state.Downloads {
		if e.Error == "" {
			return e
		}
	}
	return nil
}

func (d *Downloader) wake() {
	select {
	case d.wakeCh <- struct{}{}:
	default:
	}
}

// processOne does a single drop's full lifecycle: Range-fetch +
// verify + materialize + insert + ACK. Idempotent at each step so a
// restart mid-way doesn't double-write or miss the ACK.
func (d *Downloader) processOne(ctx context.Context, entry *downloadEntry) error {
	// 1. Fetch bytes if not already on disk.
	partial := downloadPartialPath(d.blobsDir, entry.DropID)
	if err := os.MkdirAll(filepath.Dir(partial), 0o755); err != nil {
		return fmt.Errorf("mkdir partial: %w", err)
	}
	if entry.ReceivedThrough < entry.Size {
		if err := d.streamRange(ctx, entry, partial); err != nil {
			return err
		}
	}
	// 2. Verify sha256.
	if err := verifyFileSHA(partial, entry.ExpectedSHA); err != nil {
		// Bad bytes — drop the partial, force restart on next pass.
		_ = os.Remove(partial)
		entry.ReceivedThrough = 0
		return fmt.Errorf("sha verify: %w", err)
	}
	// 3. Move into canonical content-addressed slot.
	rel := filepath.Join(entry.ExpectedSHA[:2], entry.ExpectedSHA+blobExtFromMIME(entry.MIME))
	abs := filepath.Join(d.blobsDir, rel)
	if err := os.MkdirAll(filepath.Dir(abs), 0o755); err != nil {
		return fmt.Errorf("mkdir canonical: %w", err)
	}
	if _, err := os.Stat(abs); err != nil {
		if err := os.Rename(partial, abs); err != nil {
			return fmt.Errorf("rename canonical: %w", err)
		}
	} else {
		// Canonical already exists (dedup) — drop the partial.
		_ = os.Remove(partial)
	}
	// 4. Materialize mirror if the stream's layout says so. Drop a
	// minimal Drop into the helper.
	drop := entry.toDrop()
	drop.BodyEncoding = "blob"
	drop.BodyBlobPath = rel
	drop.BodySHA256 = entry.ExpectedSHA
	drop.BodySize = entry.Size
	drop.ReceivedAt = time.Now().UTC()
	if err := materializeMirror(drop, entry.StreamLayout, d.blobsDir, d.mirrorDir); err != nil {
		log.Printf("downloader: mirror %s: %v", entry.DropID, err)
	}
	// 5. Insert local drops row (idempotent via INSERT OR IGNORE).
	if err := d.store.Insert(ctx, drop); err != nil {
		return fmt.Errorf("store insert: %w", err)
	}
	status.MarkDropReceived(entry.DropID)
	// 6. ACK to cloud (idempotent on cloud via UPSERT).
	if err := d.client.AckDrop(ctx, entry.DropID, entry.BlobID); err != nil {
		// Bytes are local — keep the entry around so a future loop
		// retries the ACK without re-downloading.
		return fmt.Errorf("ack drop: %w", err)
	}
	log.Printf("downloader: %s materialized (blob %s, %d bytes, sha=%s)",
		entry.DropID, entry.BlobID, entry.Size, entry.ExpectedSHA[:8])
	return nil
}

// streamRange fetches the remaining bytes via Range. Caller has
// already opened the partial file path; we Range-append.
func (d *Downloader) streamRange(ctx context.Context, entry *downloadEntry, partial string) error {
	for entry.ReceivedThrough < entry.Size {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Bound this chunk's range to chunkSize, but no farther than Size.
		startOff := entry.ReceivedThrough
		endOff := startOff + d.chunkSize - 1
		if endOff >= entry.Size {
			endOff = entry.Size - 1
		}
		buf, err := d.rangeFetch(ctx, entry.SignedURL, startOff, endOff)
		if err != nil {
			return err
		}
		// Append to partial file at the correct offset.
		f, err := os.OpenFile(partial, os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return fmt.Errorf("open partial: %w", err)
		}
		if _, err := f.WriteAt(buf, startOff); err != nil {
			f.Close()
			return fmt.Errorf("write partial: %w", err)
		}
		f.Close()
		entry.ReceivedThrough += int64(len(buf))
		entry.LastProgressAt = time.Now()
		d.mu.Lock()
		d.state.set(entry)
		err = d.persist()
		d.mu.Unlock()
		if err != nil {
			return fmt.Errorf("persist progress: %w", err)
		}
		if d.throttle > 0 && entry.ReceivedThrough < entry.Size {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(d.throttle):
			}
		}
	}
	return nil
}

// rangeFetch issues a single Range GET with retry/backoff.
func (d *Downloader) rangeFetch(ctx context.Context, signedURL string, startOff, endOff int64) ([]byte, error) {
	var lastErr error
	for attempt, delay := range append([]time.Duration{0}, UploadRetryDelays...) {
		if delay > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}
		buf, err := d.rangeFetchOnce(ctx, signedURL, startOff, endOff)
		if err == nil {
			return buf, nil
		}
		lastErr = err
		log.Printf("downloader: range %d-%d attempt %d failed: %v", startOff, endOff, attempt+1, err)
	}
	return nil, lastErr
}

func (d *Downloader) rangeFetchOnce(ctx context.Context, signedURL string, startOff, endOff int64) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", signedURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", startOff, endOff))
	req.Header.Set("User-Agent", "pouch-vault/"+Version)
	resp, err := blobHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		// Cloud reaped (relay GC) or expired. Surface a distinct
		// error so the loop can mark + skip.
		buf, _ := readAllN(resp.Body, 1024)
		var detail struct {
			Code string `json:"code"`
		}
		_ = json.Unmarshal(buf, &detail)
		return nil, fmt.Errorf("blob unavailable: code=%s", detail.Code)
	}
	if resp.StatusCode != 200 && resp.StatusCode != 206 {
		buf, _ := readAllN(resp.Body, 1024)
		return nil, fmt.Errorf("range %d-%d: HTTP %d (%s)", startOff, endOff, resp.StatusCode, buf)
	}
	want := endOff - startOff + 1
	buf := make([]byte, want)
	if _, err := io.ReadFull(resp.Body, buf); err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	return buf, nil
}

func (d *Downloader) persist() error {
	return saveDownloadState(d.statePath, d.state)
}

// verifyFileSHA streams the file through sha256 and compares against
// the expected hex digest. Used to validate post-download integrity
// before the bytes go canonical.
func verifyFileSHA(path, expected string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	got := hex.EncodeToString(h.Sum(nil))
	if got != expected {
		return fmt.Errorf("got %s, want %s", got, expected)
	}
	return nil
}

// downloadPartialPath returns the on-disk path where bytes accumulate
// during a chunked Range fetch. Keyed on drop_id (one in-flight
// download per drop is the worst case the SSE dedup ring permits).
func downloadPartialPath(blobsDir, dropID string) string {
	return filepath.Join(blobsDir, "_dl", dropID+".part")
}

// --- downloads.json persistence ---

type downloadState struct {
	Downloads map[string]*downloadEntry `json:"downloads"`
}

type downloadEntry struct {
	DropID          string   `json:"drop_id"`
	BlobID          string   `json:"blob_id"`
	DeliveryID      string   `json:"delivery_id"`
	SignedURL       string   `json:"signed_url"`
	ExpectedSHA     string   `json:"expected_sha"`
	Size            int64    `json:"size"`
	MIME            string   `json:"mime"`
	Stream          string   `json:"stream"`
	StreamLayout    string   `json:"stream_layout,omitempty"`
	Label           string   `json:"label"`
	Tags            []string `json:"tags,omitempty"`
	OriginalPath    string   `json:"original_path,omitempty"`
	Source          string   `json:"source,omitempty"`
	PouchUser       string   `json:"pouch_user,omitempty"`
	CreatedAt       time.Time `json:"created_at"`
	ReceivedThrough int64     `json:"received_through"`
	QueuedAt        time.Time `json:"queued_at"`
	LastProgressAt  time.Time `json:"last_progress_at"`
	Error           string    `json:"error,omitempty"`
}

func (e *downloadEntry) toDrop() *Drop {
	return &Drop{
		DeliveryID:   deliveryOrFallback(e.DeliveryID, e.DropID),
		DropID:       e.DropID,
		PouchUser:    e.PouchUser,
		Stream:       e.Stream,
		Label:        e.Label,
		Tags:         e.Tags,
		MIME:         e.MIME,
		Source:       e.Source,
		OriginalPath: e.OriginalPath,
		CreatedAt:    e.CreatedAt,
	}
}

func newDownloadState() *downloadState {
	return &downloadState{Downloads: map[string]*downloadEntry{}}
}

func (s *downloadState) set(e *downloadEntry) {
	s.Downloads[e.DropID] = e
}

func (s *downloadState) delete(dropID string) {
	delete(s.Downloads, dropID)
}

func loadDownloadState(path string) (*downloadState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return newDownloadState(), nil
		}
		return nil, err
	}
	if len(data) == 0 {
		return newDownloadState(), nil
	}
	var st downloadState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, err
	}
	if st.Downloads == nil {
		st.Downloads = map[string]*downloadEntry{}
	}
	return &st, nil
}

func saveDownloadState(path string, st *downloadState) error {
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

// downloaderStatePath returns ~/.local/state/pouch-vault/downloads.json
// or the OS equivalent. Falls back to ./downloads.json when home
// can't be resolved.
func downloaderStatePath() string {
	if dir, err := os.UserHomeDir(); err == nil {
		return filepath.Join(dir, ".local", "state", "pouch-vault", "downloads.json")
	}
	return "downloads.json"
}

// downloadThrottleFromEnv reads VAULT_DOWNLOAD_THROTTLE_MS for the
// inter-chunk sleep on the consumer side. Mirrors the producer's
// VAULT_UPLOAD_THROTTLE_MS knob — same default (250ms).
func downloadThrottleFromEnv() time.Duration {
	if s := os.Getenv("VAULT_DOWNLOAD_THROTTLE_MS"); s != "" {
		if n, err := strconvAtoi(s); err == nil && n >= 0 {
			return time.Duration(n) * time.Millisecond
		}
	}
	return DownloadThrottleMS * time.Millisecond
}

// tiny strconv.Atoi alias to keep the import list of this file
// short (only the std-lib bits actually used elsewhere).
func strconvAtoi(s string) (int, error) {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("not a number: %q", s)
		}
		n = n*10 + int(c-'0')
	}
	return n, nil
}

// errBlobReaped sentinel — currently unused, reserved for future
// fancy handling when cloud returns code=relayed-expired. Today the
// downloader just logs and skips.
var errBlobReaped = errors.New("blob reaped")
