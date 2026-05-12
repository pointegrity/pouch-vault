package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

// payload mirrors what pouch POSTs (see /docs/webhooks).
type payload struct {
	Event     string    `json:"event"`
	PouchUser string    `json:"pouch_user"`
	Stream    string    `json:"stream"`
	SentAt    time.Time `json:"sent_at"`
	Drop      payloadDrop `json:"drop"`
}

type payloadDrop struct {
	ID           string         `json:"id"`
	Label        string         `json:"label"`
	Body         string         `json:"body"`
	BodyEncoding string         `json:"body_encoding,omitempty"`
	// Blob, when present, points at a server-side blob the receiver
	// should fetch out-of-band. URL is signed + time-limited (5 min).
	Blob         *payloadBlob   `json:"blob,omitempty"`
	Tags         []string       `json:"tags,omitempty"`
	MIME         string         `json:"mime,omitempty"`
	Source       string         `json:"source,omitempty"`
	// OriginalPath: producer-stamped path (Phase 5 slice 8c).
	// Empty for drops produced by non-vault flows; populated by
	// pouch-vault sync/watch and pouch-vault-git put.
	OriginalPath string         `json:"original_path,omitempty"`
	CreatedAt    time.Time      `json:"created_at"`
}

type payloadBlob struct {
	URL    string `json:"url"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// Receiver is the HTTP handler half of pouch-vault: receives
// webhook deliveries from pouch, verifies the HMAC, dedups by
// X-Pouch-Delivery, and writes to the local store.
type Receiver struct {
	store      *Store
	hmacSecret string
	blobsDir   string
	dedup      *dedupRing
}

// NewReceiver constructs the receiver bound to a concrete store +
// shared HMAC secret. The secret was minted by `pouch vault create`
// on the server side and copied into the vault's config.
func NewReceiver(store *Store, hmacSecret, blobsDir string) *Receiver {
	return &Receiver{
		store:      store,
		hmacSecret: hmacSecret,
		blobsDir:   blobsDir,
		dedup:      newDedupRing(1024),
	}
}

// Handler returns an http.HandlerFunc for /hook.
func (r *Receiver) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			http.Error(w, "read", 400)
			return
		}
		if err := r.verify(body, req.Header.Get("X-Pouch-Signature")); err != nil {
			log.Printf("rejected: %v", err)
			http.Error(w, "bad signature", 401)
			return
		}
		// Dedup: a retried pouch delivery has the same X-Pouch-Delivery
		// as the original. Skip silently with 200 so pouch stops
		// retrying.
		delivery := req.Header.Get("X-Pouch-Delivery")
		if r.dedup.Seen(delivery) {
			w.WriteHeader(200)
			return
		}
		var p payload
		if err := json.Unmarshal(body, &p); err != nil {
			http.Error(w, "bad json", 400)
			return
		}
		drop := &Drop{
			DeliveryID:   deliveryOrFallback(delivery, p.Drop.ID),
			DropID:       p.Drop.ID,
			PouchUser:    p.PouchUser,
			Stream:       p.Stream,
			Label:        p.Drop.Label,
			Body:         p.Drop.Body,
			BodyEncoding: p.Drop.BodyEncoding,
			Tags:         p.Drop.Tags,
			MIME:         p.Drop.MIME,
			Source:       p.Drop.Source,
			OriginalPath: p.Drop.OriginalPath,
			CreatedAt:    p.Drop.CreatedAt,
			ReceivedAt:   time.Now().UTC(),
		}
		if err := materializeBlob(drop, r.blobsDir); err != nil {
			log.Printf("recv: materialize %s: %v", drop.DropID, err)
		}
		if err := r.store.Insert(req.Context(), drop); err != nil {
			log.Printf("store insert %s: %v", drop.DropID, err)
			http.Error(w, "store error", 500)
			return
		}
		status.MarkDropReceived(drop.DropID)
		w.WriteHeader(200)
	}
}

// verify is constant-time HMAC-SHA256 compare against the body. An
// empty hmacSecret skips verification — useful only for local
// development.
func (r *Receiver) verify(body []byte, header string) error {
	if r.hmacSecret == "" {
		return nil
	}
	got := strings.TrimPrefix(header, "sha256=")
	mac := hmac.New(sha256.New, []byte(r.hmacSecret))
	mac.Write(body)
	want := hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(got), []byte(want)) {
		return fmt.Errorf("hmac mismatch")
	}
	return nil
}

// deliveryOrFallback returns delivery if non-empty, else a fallback
// derived from the drop id so the (delivery_id, drop_id) primary key
// still gets a non-empty value for ancient pouches that don't ship
// the X-Pouch-Delivery header. (Modern pouch always does.)
func deliveryOrFallback(delivery, dropID string) string {
	if delivery != "" {
		return delivery
	}
	return "no-delivery-" + dropID
}

// dedupRing is a tiny last-N seen ring keyed on X-Pouch-Delivery
// strings. Big enough for pouch's 4-attempts × 30s/2m/8m retry
// budget at any reasonable drop rate; small enough to live entirely
// in memory.
type dedupRing struct {
	mu   sync.Mutex
	seen map[string]struct{}
	ring []string
	max  int
}

func newDedupRing(size int) *dedupRing {
	if size <= 0 {
		size = 1024
	}
	return &dedupRing{seen: make(map[string]struct{}, size), max: size}
}

func (d *dedupRing) Seen(id string) bool {
	if id == "" {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.seen[id]; ok {
		return true
	}
	d.seen[id] = struct{}{}
	d.ring = append(d.ring, id)
	if len(d.ring) > d.max {
		drop := d.ring[0]
		d.ring = d.ring[1:]
		delete(d.seen, drop)
	}
	return false
}
