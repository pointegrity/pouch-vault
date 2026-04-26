package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// Drop is the row we persist per webhook delivery.
type Drop struct {
	DeliveryID string // X-Pouch-Delivery (idempotency)
	DropID     string // pouch's itm-... id
	PouchUser  string
	Stream     string
	Label      string
	Body       string
	Tags       []string
	MIME       string
	Source     string
	CreatedAt  time.Time
	ReceivedAt time.Time
}

// Store is the local sqlite archive (drops + bookkeeping).
type Store struct {
	db *sql.DB
}

const schemaSQL = `
CREATE TABLE IF NOT EXISTS drops (
  delivery_id TEXT PRIMARY KEY,
  drop_id     TEXT NOT NULL,
  pouch_user  TEXT NOT NULL,
  stream      TEXT NOT NULL,
  label       TEXT,
  body        TEXT,
  tags        TEXT,             -- JSON array
  mime        TEXT,
  source      TEXT,
  created_at  DATETIME NOT NULL,
  received_at DATETIME NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_drops_id     ON drops(drop_id);
CREATE INDEX IF NOT EXISTS idx_drops_us     ON drops(pouch_user, stream, received_at DESC);

-- Append-only audit. Useful for "did this drop arrive?" forensics
-- and for the heartbeat report (last drop id seen).
`

// OpenStore creates / opens an anchor's local DB. _journal=WAL keeps
// reads non-blocking while the receiver writes; _busy_timeout buys
// a few seconds of automatic retry on transient lock contention.
func OpenStore(path string) (*Store, error) {
	// modernc/sqlite uses _pragma= for connection-level pragmas
	// (different from mattn's _journal=...). Equivalent semantics.
	dsn := path + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	// One writer is fine; sqlite serialises anyway. Avoids lock
	// timeouts during the single goroutine's bursts.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schemaSQL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("schema: %w", err)
	}
	return &Store{db: db}, nil
}

// Close releases the underlying DB.
func (s *Store) Close() error { return s.db.Close() }

// Insert writes a drop. Idempotent on (delivery_id) — a retried
// pouch delivery returns no error and inserts no row.
func (s *Store) Insert(ctx context.Context, d *Drop) error {
	tagsJSON := "[]"
	if len(d.Tags) > 0 {
		if b, err := json.Marshal(d.Tags); err == nil {
			tagsJSON = string(b)
		}
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO drops
		  (delivery_id, drop_id, pouch_user, stream, label, body, tags,
		   mime, source, created_at, received_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, d.DeliveryID, d.DropID, d.PouchUser, d.Stream,
		d.Label, d.Body, tagsJSON, d.MIME, d.Source,
		d.CreatedAt, d.ReceivedAt)
	return err
}

// List returns up to `limit` drops, newest first. If `search` is
// non-empty, filters with a LIKE over label and body. Used by the
// local UI; not by the daemon's main path.
func (s *Store) List(ctx context.Context, search string, limit int) ([]Drop, error) {
	if limit <= 0 || limit > 500 {
		limit = 50
	}
	var (
		rows interface {
			Next() bool
			Scan(...any) error
			Err() error
			Close() error
		}
		err error
	)
	if search == "" {
		r, e := s.db.QueryContext(ctx, `
			SELECT delivery_id, drop_id, pouch_user, stream, label, body, tags,
			       mime, source, created_at, received_at
			FROM drops
			ORDER BY received_at DESC
			LIMIT ?
		`, limit)
		rows, err = r, e
	} else {
		like := "%" + search + "%"
		r, e := s.db.QueryContext(ctx, `
			SELECT delivery_id, drop_id, pouch_user, stream, label, body, tags,
			       mime, source, created_at, received_at
			FROM drops
			WHERE label LIKE ? OR body LIKE ?
			ORDER BY received_at DESC
			LIMIT ?
		`, like, like, limit)
		rows, err = r, e
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Drop{}
	for rows.Next() {
		var d Drop
		var tagsStr string
		if err := rows.Scan(&d.DeliveryID, &d.DropID, &d.PouchUser, &d.Stream,
			&d.Label, &d.Body, &tagsStr, &d.MIME, &d.Source,
			&d.CreatedAt, &d.ReceivedAt); err != nil {
			return nil, err
		}
		if tagsStr != "" && tagsStr != "[]" {
			_ = json.Unmarshal([]byte(tagsStr), &d.Tags)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// Get returns one drop by drop_id (the pouch-side itm-... id). The
// id is unique-enough for our purposes; in the unlikely event of a
// collision (would require pouch sending the same drop_id twice with
// different delivery_ids), we return the most recent.
func (s *Store) Get(ctx context.Context, dropID string) (*Drop, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT delivery_id, drop_id, pouch_user, stream, label, body, tags,
		       mime, source, created_at, received_at
		FROM drops
		WHERE drop_id = ?
		ORDER BY received_at DESC
		LIMIT 1
	`, dropID)
	var d Drop
	var tagsStr string
	err := row.Scan(&d.DeliveryID, &d.DropID, &d.PouchUser, &d.Stream,
		&d.Label, &d.Body, &tagsStr, &d.MIME, &d.Source,
		&d.CreatedAt, &d.ReceivedAt)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return nil, nil
		}
		return nil, err
	}
	if tagsStr != "" && tagsStr != "[]" {
		_ = json.Unmarshal([]byte(tagsStr), &d.Tags)
	}
	return &d, nil
}

// Stats returns total row count + the most recent drop id for the
// heartbeat report. Cheap — `count(*)` over a small table is fast,
// and the index makes the order-by trivial.
func (s *Store) Stats(ctx context.Context) (count int64, lastDropID string, err error) {
	row := s.db.QueryRowContext(ctx, `SELECT count(*) FROM drops`)
	if err = row.Scan(&count); err != nil {
		return 0, "", err
	}
	if count == 0 {
		return 0, "", nil
	}
	var id sql.NullString
	row = s.db.QueryRowContext(ctx, `
		SELECT drop_id FROM drops ORDER BY received_at DESC LIMIT 1
	`)
	if err := row.Scan(&id); err != nil && err != sql.ErrNoRows {
		return count, "", err
	}
	return count, id.String, nil
}
