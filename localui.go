// Local UI: a tiny localhost-only viewer for the anchor's status
// and the contents of its archive.
//
// Bound to whatever ANCHOR_LISTEN serves (default 127.0.0.1:7780).
// No auth: it's localhost. Read-only: no delete, no share, no put.
// Anything mutating, the user does in their pouch SaaS where they're
// already logged in.
//
// Routes:
//   GET /ui                    — single-file HTML page (vanilla JS)
//   GET /api/local/status      — anchor's runtime status (JSON)
//   GET /api/local/drops       — recent drops, optional ?q=, ?limit=
//   GET /api/local/drops/{id}  — one drop's full record + body
package main

import (
	_ "embed"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"strconv"
	"time"
)

//go:embed localui.html
var localUIHTML []byte

// mountLocalUI registers the handlers on mux. Caller passes the
// live store + the on-disk blobs directory. Both push and pull
// modes call this.
func mountLocalUI(mux *http.ServeMux, st *Store, blobsDir string) {
	mux.HandleFunc("GET /ui", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(localUIHTML)
	})
	// `GET /` -> redirect to /ui for friendliness.
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui", http.StatusFound)
	})

	mux.HandleFunc("GET /api/local/status", func(w http.ResponseWriter, r *http.Request) {
		count, lastID, err := st.Stats(r.Context())
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		out := map[string]any{
			"anchor_name": status.AnchorName,
			"anchor_id":   status.AnchorID,
			"version":     status.Version,
			"mode":        status.Mode,
			"hostname":    status.Hostname,
			"pouch_url":   status.PouchURL,
			"db_path":     status.DBPath,
			"started_at":  status.StartedAt.UTC(),

			"stream_connected":      status.streamConnected.Load(),
			"last_connected_at":     unixNanoToISO(status.lastConnectedAt.Load()),
			"last_disconnect":       derefString(status.lastDisconnect.Load()),
			"last_heartbeat_at":     unixNanoToISO(status.lastHeartbeatAt.Load()),
			"last_heartbeat_error":  derefString(status.lastHeartbeatError.Load()),
			"last_drop_received_at": unixNanoToISO(status.lastDropReceivedAt.Load()),
			"last_drop_id":          derefString(status.lastDropID.Load()),

			"total_drops":      count,
			"last_drop_stored": lastID,
		}
		writeJSON(w, 200, out)
	})

	mux.HandleFunc("GET /api/local/drops", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query().Get("q")
		limit := 50
		if l := r.URL.Query().Get("limit"); l != "" {
			if n, err := strconv.Atoi(l); err == nil {
				limit = n
			}
		}
		drops, err := st.List(r.Context(), q, limit)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		writeJSON(w, 200, map[string]any{"drops": drops})
	})

	mux.HandleFunc("GET /api/local/drops/{id}", func(w http.ResponseWriter, r *http.Request) {
		drop, err := st.Get(r.Context(), r.PathValue("id"))
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		if drop == nil {
			http.NotFound(w, r)
			return
		}
		writeJSON(w, 200, drop)
	})

	// Blob streaming for materialized binaries. The sha256 is the
	// path parameter; we glob the sharded subdir to find the file
	// (extension is cosmetic, so we don't need to know it). Sets
	// Content-Type from the first row that uses this blob — useful
	// for browsers rendering <img src="…/blobs/<sha>">.
	mux.HandleFunc("GET /api/local/blobs/{sha}", func(w http.ResponseWriter, r *http.Request) {
		sha := r.PathValue("sha")
		// Defensive: only hex digits, length 64. No path traversal.
		if !looksLikeSha256(sha) {
			http.Error(w, "bad sha", 400)
			return
		}
		body, _, err := readBlob(blobsDir, sha)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				http.NotFound(w, r)
				return
			}
			http.Error(w, err.Error(), 500)
			return
		}
		// Look up the mime by querying the store for any drop
		// pointing at this blob. Cheap; one row.
		mimeStr := "application/octet-stream"
		if d, _ := st.GetByBlobSHA(r.Context(), sha); d != nil && d.MIME != "" {
			mimeStr = d.MIME
		}
		w.Header().Set("Content-Type", mimeStr)
		w.Header().Set("Cache-Control", "private, max-age=3600")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		_, _ = w.Write(body)
	})
}

// looksLikeSha256 returns true for a 64-char lowercase hex string.
func looksLikeSha256(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9':
		case c >= 'a' && c <= 'f':
		default:
			return false
		}
	}
	return true
}


func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func unixNanoToISO(n int64) any {
	if n == 0 {
		return nil
	}
	return time.Unix(0, n).UTC().Format(time.RFC3339)
}

func derefString(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
