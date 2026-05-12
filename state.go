// sync state file — tracks which files in each watched path the
// vault has already produced as drops, so subsequent sync runs only
// see real changes. Layout (atomic-replace on write):
//
//   <data-dir>/sync.json
//
// Shape:
//   {
//     "version": 1,
//     "paths": {
//       "<watch-path>": {
//         "scanned_at": "RFC3339",
//         "files": {
//           "<relative-file>": {
//             "sha256":   "...",
//             "size":     12345,
//             "mtime":    "RFC3339",
//             "drop_id":  "itm-...",
//             "stream":   "..."
//           }
//         }
//       }
//     }
//   }
//
// Per decision vault-producer-mode-and-local-only-git.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

const syncStateVersion = 1

type syncState struct {
	Version int                          `json:"version"`
	Paths   map[string]*syncStatePath    `json:"paths"`
}

type syncStatePath struct {
	ScannedAt time.Time                       `json:"scanned_at"`
	Files     map[string]*syncStateFile       `json:"files"`
}

type syncStateFile struct {
	SHA256 string    `json:"sha256"`
	Size   int64     `json:"size"`
	MTime  time.Time `json:"mtime"`
	DropID string    `json:"drop_id"`
	Stream string    `json:"stream"`
}

// syncStatePath returns the syncStateFile for a path's file, or nil
// when the file hasn't been seen.
func (s *syncState) get(watchPath, relFile string) *syncStateFile {
	if s == nil || s.Paths == nil {
		return nil
	}
	p := s.Paths[watchPath]
	if p == nil || p.Files == nil {
		return nil
	}
	return p.Files[relFile]
}

func (s *syncState) set(watchPath, relFile string, f *syncStateFile) {
	if s.Paths == nil {
		s.Paths = map[string]*syncStatePath{}
	}
	p := s.Paths[watchPath]
	if p == nil {
		p = &syncStatePath{Files: map[string]*syncStateFile{}}
		s.Paths[watchPath] = p
	}
	if p.Files == nil {
		p.Files = map[string]*syncStateFile{}
	}
	p.Files[relFile] = f
}

// syncStatePathFile returns the canonical state-file path. The
// directory is the same XDG / OS-conventional data dir the SQLite
// DB uses, so backup tools that capture one capture both.
func syncStatePathFile() (string, error) {
	d, err := dataDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(d, "sync.json"), nil
}

// loadSyncState reads the state file. A missing file is fine —
// returns an empty state (first-run semantics).
func loadSyncState() (*syncState, error) {
	path, err := syncStatePathFile()
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return &syncState{Version: syncStateVersion, Paths: map[string]*syncStatePath{}}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var s syncState
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if s.Version == 0 {
		s.Version = syncStateVersion
	}
	if s.Paths == nil {
		s.Paths = map[string]*syncStatePath{}
	}
	return &s, nil
}

// saveSyncState writes the state file atomically (write to tmp,
// rename). Caller passes the in-memory struct; nil is treated as
// empty.
func saveSyncState(s *syncState) error {
	if s == nil {
		s = &syncState{Version: syncStateVersion, Paths: map[string]*syncStatePath{}}
	}
	path, err := syncStatePathFile()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
