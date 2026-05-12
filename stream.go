package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// runStream is the pull-mode loop: keep an SSE connection open to
// /api/vaults/stream and dispatch each incoming drop.created event
// to the local store.
//
// Reconnects with backoff on disconnect: 2s, 4s, 8s, 16s, capped at
// 60s. Pouch's keep-alive comments every 25s keep the connection
// fresh through proxies; anything longer than that without traffic
// triggers a reconnect.
func runStream(ctx context.Context, client *PouchClient, store *Store, hmacSecret, blobsDir, mirrorDir string) {
	dedup := newDedupRing(1024)
	delay := 2 * time.Second
	const maxDelay = 60 * time.Second

	for {
		if ctx.Err() != nil {
			return
		}
		err := streamOnce(ctx, client, store, hmacSecret, blobsDir, mirrorDir, dedup)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			log.Printf("stream: %v (reconnecting in %s)", err, delay)
		} else {
			// Clean disconnect (server closed). Reset backoff.
			delay = 2 * time.Second
			log.Printf("stream: server closed connection; reconnecting in %s", delay)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		// Exponential backoff for repeated failures, capped.
		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}
}

// streamOnce holds one SSE connection until it ends. Returns the
// reason the connection ended; nil for a clean server-side close.
func streamOnce(ctx context.Context, client *PouchClient, store *Store, hmacSecret, blobsDir, mirrorDir string, dedup *dedupRing) error {
	req, err := http.NewRequestWithContext(ctx, "GET",
		client.BaseURL+"/api/vaults/stream", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("X-Vault-Key", client.APIKey)
	req.Header.Set("User-Agent", client.UserAgent)

	// SSE = long-lived response; the client must NOT impose a
	// short read timeout. Use a separate client with no timeout
	// for the streaming request.
	streamClient := &http.Client{Timeout: 0}
	resp, err := streamClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		buf, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("stream %d: %s", resp.StatusCode, strings.TrimSpace(string(buf)))
	}
	log.Printf("stream: connected to %s/api/vaults/stream", client.BaseURL)
	status.MarkConnected()
	defer status.MarkDisconnected("loop ended")

	parser := newSSEParser(resp.Body)
	for {
		ev, err := parser.next()
		if err != nil {
			return err
		}
		if ev == nil {
			continue
		}
		if ev.event != "drop.created" {
			// keep-alives, comments, and other event types — ignore.
			continue
		}
		if dedup.Seen(ev.id) {
			continue
		}
		if err := handleStreamDrop(ctx, store, hmacSecret, blobsDir, mirrorDir, ev); err != nil {
			log.Printf("stream: drop %s: %v", ev.id, err)
			// Keep going — one bad event shouldn't sink the loop.
		}
	}
}

// handleStreamDrop verifies the HMAC, decodes, and persists. The
// wire wrapper sent by pouch is:
//
//	{"sig":"sha256=...","delivery":"<uuid>","body":"<json string>"}
//
// `body` is the raw JSON the webhook receiver would have parsed —
// HMAC is computed over those bytes.
func handleStreamDrop(ctx context.Context, store *Store, hmacSecret, blobsDir, mirrorDir string, ev *sseEvent) error {
	var wrapper struct {
		Sig      string `json:"sig"`
		Delivery string `json:"delivery"`
		Body     string `json:"body"`
	}
	if err := json.Unmarshal([]byte(ev.data), &wrapper); err != nil {
		return fmt.Errorf("decode wrapper: %w", err)
	}
	r := &Receiver{hmacSecret: hmacSecret}
	if err := r.verify([]byte(wrapper.Body), wrapper.Sig); err != nil {
		return err
	}
	var p payload
	if err := json.Unmarshal([]byte(wrapper.Body), &p); err != nil {
		return fmt.Errorf("decode payload: %w", err)
	}
	delivery := wrapper.Delivery
	if delivery == "" {
		delivery = ev.id
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
	// Phase 2: server handed us a manifest instead of inline bytes.
	// Fetch the blob from the signed URL, verify sha256, write to
	// disk, mark the drop as 'blob'-encoded.
	if p.Drop.Blob != nil {
		rel, err := fetchBlobToDisk(ctx, blobsDir,
			p.Drop.Blob.URL, p.Drop.Blob.SHA256, p.Drop.MIME, p.Drop.Blob.Size)
		if err != nil {
			return fmt.Errorf("blob fetch %s: %w", drop.DropID, err)
		}
		drop.BodyEncoding = "blob"
		drop.BodyBlobPath = rel
		drop.BodySHA256 = p.Drop.Blob.SHA256
		drop.BodySize = p.Drop.Blob.Size
		drop.Body = ""
	} else if err := materializeBlob(drop, blobsDir); err != nil {
		// Phase 1B: inline base64 spilled to disk for non-tiny binaries.
		log.Printf("stream: materialize %s: %v", drop.DropID, err)
	}
	if err := materializeMirror(drop, p.StreamLayout, blobsDir, mirrorDir); err != nil {
		log.Printf("stream: mirror %s: %v", drop.DropID, err)
	}
	if err := store.Insert(ctx, drop); err != nil {
		return err
	}
	status.MarkDropReceived(drop.DropID)
	return nil
}

// --- minimal SSE parser ------------------------------------------------
//
// SSE is line-oriented:
//   field: value
//   field: value
//   <blank line>     ← dispatch the accumulated event
//
// A leading colon is a comment (used for keep-alives). We only need
// id / event / data fields; everything else is ignored.

type sseEvent struct {
	id    string
	event string
	data  string
}

type sseParser struct {
	r *bufio.Reader
}

func newSSEParser(r io.Reader) *sseParser {
	// Larger buffer than default — drop bodies can be longer than
	// bufio's default 4 KB.
	return &sseParser{r: bufio.NewReaderSize(r, 1<<16)}
}

// next blocks until either one full event is read (returned non-nil)
// or the stream ends. nil event + nil error means "still reading,
// nothing to dispatch yet".
func (p *sseParser) next() (*sseEvent, error) {
	var ev sseEvent
	dataLines := 0
	var dataBuf strings.Builder
	for {
		line, err := p.r.ReadString('\n')
		if err != nil {
			// Treat io.EOF (server closed) as a clean close.
			if err == io.EOF {
				return nil, nil
			}
			return nil, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			// Dispatch the accumulated event, if any.
			if ev.event == "" && ev.id == "" && dataLines == 0 {
				continue
			}
			ev.data = dataBuf.String()
			return &ev, nil
		}
		if strings.HasPrefix(line, ":") {
			continue // comment / keep-alive
		}
		colon := strings.IndexByte(line, ':')
		var field, value string
		if colon == -1 {
			field = line
		} else {
			field = line[:colon]
			value = line[colon+1:]
			if strings.HasPrefix(value, " ") {
				value = value[1:]
			}
		}
		switch field {
		case "id":
			ev.id = value
		case "event":
			ev.event = value
		case "data":
			if dataLines > 0 {
				dataBuf.WriteByte('\n')
			}
			dataBuf.WriteString(value)
			dataLines++
		}
		// Ignore retry/etc.
	}
}
