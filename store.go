package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Drop is the row we persist per webhook delivery.
type Drop struct {
	DeliveryID   string // X-Pouch-Delivery (idempotency)
	DropID       string // pouch's itm-... id
	PouchUser    string
	Stream       string
	Label        string

	// Body is the inline payload — utf8 text or base64 binary —
	// when small enough to live in SQLite. For binary drops over
	// the inline threshold it's empty and the canonical bytes live
	// at BodyBlobPath.
	Body         string
	BodyEncoding string // "utf8" | "base64" | "blob"

	// Set when BodyEncoding == "blob": canonical SHA-256 of the
	// decoded bytes, the on-disk relative path, and the byte size.
	// The local /api/local/blobs/{sha256} route streams the file.
	BodySHA256   string
	BodyBlobPath string
	BodySize     int64

	Tags         []string
	MIME         string
	Source       string
	// OriginalPath is the source-side path the drop came from
	// (e.g. "scrapes/example.com/page.html" or "session-1.json").
	// Populated from the WebhookDrop.OriginalPath field that
	// producer flows (pouch-vault sync, pouch-vault-git put)
	// stamp on the cloud-side item. Used by `pouch-vault history
	// --path <orig>` to find every version of a logical file.
	OriginalPath string
	CreatedAt    time.Time
	ReceivedAt   time.Time
}

// Store is the local sqlite archive (drops + bookkeeping).
type Store struct {
	db *sql.DB
}

const schemaSQL = `
CREATE TABLE IF NOT EXISTS drops (
  delivery_id   TEXT PRIMARY KEY,
  drop_id       TEXT NOT NULL,
  pouch_user    TEXT NOT NULL,
  stream        TEXT NOT NULL,
  label         TEXT,
  body          TEXT,
  body_encoding TEXT NOT NULL DEFAULT 'utf8',
  tags          TEXT,             -- JSON array
  mime          TEXT,
  source        TEXT,
  created_at    DATETIME NOT NULL,
  received_at   DATETIME NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_drops_id     ON drops(drop_id);
CREATE INDEX IF NOT EXISTS idx_drops_us     ON drops(pouch_user, stream, received_at DESC);
`

// migrateSchema applies idempotent ALTER TABLEs for upgrades from
// older vault builds. New columns added since v0.5.0 land here.
const migrateSQL = `
ALTER TABLE drops ADD COLUMN body_encoding TEXT NOT NULL DEFAULT 'utf8';
ALTER TABLE drops ADD COLUMN body_sha256 TEXT;
ALTER TABLE drops ADD COLUMN body_blob_path TEXT;
ALTER TABLE drops ADD COLUMN body_size INTEGER NOT NULL DEFAULT 0;
ALTER TABLE drops ADD COLUMN original_path TEXT;
CREATE INDEX IF NOT EXISTS idx_drops_orig ON drops(original_path) WHERE original_path IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_drops_label ON drops(label) WHERE label IS NOT NULL;
`

// OpenStore creates / opens a vault's local DB. _journal=WAL keeps
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
	// Idempotent upgrades. ALTER TABLE on a column that already
	// exists returns "duplicate column" — swallow that, fail on
	// anything else.
	for _, stmt := range strings.Split(migrateSQL, ";") {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		if _, err := db.Exec(stmt); err != nil &&
			!strings.Contains(err.Error(), "duplicate column") {
			_ = db.Close()
			return nil, fmt.Errorf("migrate %q: %w", stmt, err)
		}
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
	enc := d.BodyEncoding
	if enc == "" {
		enc = "utf8"
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT OR IGNORE INTO drops
		  (delivery_id, drop_id, pouch_user, stream, label, body, body_encoding,
		   body_sha256, body_blob_path, body_size, tags,
		   mime, source, original_path, created_at, received_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, d.DeliveryID, d.DropID, d.PouchUser, d.Stream,
		d.Label, d.Body, enc,
		nullStr(d.BodySHA256), nullStr(d.BodyBlobPath), d.BodySize,
		tagsJSON, d.MIME, d.Source, nullStr(d.OriginalPath),
		d.CreatedAt, d.ReceivedAt)
	return err
}

// HistoryFilter narrows ListHistory. Zero-value lists all drops.
// Combine with AND.
type HistoryFilter struct {
	Stream string // exact stream name
	Label  string // case-insensitive substring match
	Path   string // exact original_path match
	Limit  int    // 0 -> 50
}

// ListHistory returns drops matching the filter, newest first.
// Used by `pouch-vault history` to surface the catalog's free
// versioning (multiple drops with the same label / original_path
// = a version chain). Phase 5 slice 8c.
func (s *Store) ListHistory(ctx context.Context, f HistoryFilter) ([]Drop, error) {
	q := `
		SELECT delivery_id, drop_id, pouch_user, stream, label, body, body_encoding,
		       body_sha256, body_blob_path, body_size, tags,
		       mime, source, original_path, created_at, received_at
		FROM drops
		WHERE 1=1`
	args := []any{}
	if f.Stream != "" {
		q += ` AND stream = ?`
		args = append(args, f.Stream)
	}
	if f.Label != "" {
		q += ` AND LOWER(label) LIKE ?`
		args = append(args, "%"+strings.ToLower(f.Label)+"%")
	}
	if f.Path != "" {
		q += ` AND original_path = ?`
		args = append(args, f.Path)
	}
	limit := f.Limit
	if limit <= 0 {
		limit = 50
	}
	q += ` ORDER BY received_at DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Drop{}
	for rows.Next() {
		var d Drop
		var tagsStr string
		var bsha, blobPath, origPath sql.NullString
		if err := rows.Scan(&d.DeliveryID, &d.DropID, &d.PouchUser, &d.Stream,
			&d.Label, &d.Body, &d.BodyEncoding,
			&bsha, &blobPath, &d.BodySize,
			&tagsStr, &d.MIME, &d.Source, &origPath,
			&d.CreatedAt, &d.ReceivedAt); err != nil {
			return nil, err
		}
		d.BodySHA256 = bsha.String
		d.BodyBlobPath = blobPath.String
		d.OriginalPath = origPath.String
		if tagsStr != "" && tagsStr != "[]" {
			_ = json.Unmarshal([]byte(tagsStr), &d.Tags)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func nullStr(s string) any {
	if s == "" {
		return nil
	}
	return s
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
		var sha, blobPath sql.NullString
		if err := rows.Scan(&d.DeliveryID, &d.DropID, &d.PouchUser, &d.Stream,
			&d.Label, &d.Body, &d.BodyEncoding,
			&sha, &blobPath, &d.BodySize,
			&tagsStr, &d.MIME, &d.Source,
			&d.CreatedAt, &d.ReceivedAt); err != nil {
			return nil, err
		}
		d.BodySHA256 = sha.String
		d.BodyBlobPath = blobPath.String
		if tagsStr != "" && tagsStr != "[]" {
			_ = json.Unmarshal([]byte(tagsStr), &d.Tags)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// GetByBlobSHA returns the most recent drop pointing at a blob with
// this sha256 — used by the local UI's /blobs/{sha} handler to
// figure out the right Content-Type to serve the bytes with.
// (nil, nil) when no drop references it.
func (s *Store) GetByBlobSHA(ctx context.Context, sha string) (*Drop, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT delivery_id, drop_id, pouch_user, stream, label, body, body_encoding,
		       body_sha256, body_blob_path, body_size, tags,
		       mime, source, created_at, received_at
		FROM drops
		WHERE body_sha256 = ?
		ORDER BY received_at DESC
		LIMIT 1
	`, sha)
	var d Drop
	var tagsStr string
	var bsha, blobPath sql.NullString
	err := row.Scan(&d.DeliveryID, &d.DropID, &d.PouchUser, &d.Stream,
		&d.Label, &d.Body, &d.BodyEncoding,
		&bsha, &blobPath, &d.BodySize,
		&tagsStr, &d.MIME, &d.Source,
		&d.CreatedAt, &d.ReceivedAt)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return nil, nil
		}
		return nil, err
	}
	d.BodySHA256 = bsha.String
	d.BodyBlobPath = blobPath.String
	return &d, nil
}

// Get returns one drop by drop_id (the pouch-side itm-... id). The
// id is unique-enough for our purposes; in the unlikely event of a
// collision (would require pouch sending the same drop_id twice with
// different delivery_ids), we return the most recent.
func (s *Store) Get(ctx context.Context, dropID string) (*Drop, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT delivery_id, drop_id, pouch_user, stream, label, body, body_encoding,
		       body_sha256, body_blob_path, body_size, tags,
		       mime, source, original_path, created_at, received_at
		FROM drops
		WHERE drop_id = ?
		ORDER BY received_at DESC
		LIMIT 1
	`, dropID)
	var d Drop
	var tagsStr string
	var sha, blobPath, origPath sql.NullString
	err := row.Scan(&d.DeliveryID, &d.DropID, &d.PouchUser, &d.Stream,
		&d.Label, &d.Body, &d.BodyEncoding,
		&sha, &blobPath, &d.BodySize,
		&tagsStr, &d.MIME, &d.Source, &origPath,
		&d.CreatedAt, &d.ReceivedAt)
	d.BodySHA256 = sha.String
	d.BodyBlobPath = blobPath.String
	d.OriginalPath = origPath.String
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
