package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// PouchClient is the outbound side: vault → pouch.
//
// Only two endpoints today (register, heartbeat). Auth is the
// long-lived API key minted by `pouch vault create`, sent in
// the X-Vault-Key header.
type PouchClient struct {
	BaseURL    string        // e.g. "https://pouch.pointegrity.com"
	APIKey     string        // pk_... from `pouch vault create`
	HTTPClient *http.Client  // nil → default with 10s timeout
	UserAgent  string
}

// NewPouchClient returns a client wired with sensible defaults.
func NewPouchClient(baseURL, apiKey string) *PouchClient {
	return &PouchClient{
		BaseURL:    strings.TrimRight(baseURL, "/"),
		APIKey:     apiKey,
		HTTPClient: &http.Client{Timeout: 10 * time.Second},
		UserAgent:  "pouch-vault/0.1.0",
	}
}

// Register tells pouch our public URL + identity + declared paths.
// Idempotent; called on every vault boot before the heartbeat loop
// starts. The paths argument carries this vault's multi-folder
// declaration — cloud reconciles channels from it (decision
// vault-declares-paths-cloud-reflects-channels). Empty paths is
// fine; vault pairs but routes nothing until configured.
func (c *PouchClient) Register(ctx context.Context, publicURL, hostname, version string, paths []RegisterPath) (vaultID string, err error) {
	in := map[string]any{
		"public_url": publicURL,
		"hostname":   hostname,
		"version":    version,
	}
	if len(paths) > 0 {
		in["paths"] = paths
	}
	out := struct {
		VaultID string `json:"vault_id"`
	}{}
	if err := c.post(ctx, "/api/vaults/register", in, &out); err != nil {
		return "", err
	}
	return out.VaultID, nil
}

// RegisterPath is one path declaration sent in Register payload.
// Same shape as cloud's store.VaultPath (cloud accepts label too).
type RegisterPath struct {
	Path   string `json:"path"`
	Stream string `json:"stream"`
	Label  string `json:"label,omitempty"`
}

// Heartbeat reports last-drop / total-drops to pouch. Optionally
// includes per-path counts (vault_path_activity rolling window on
// the cloud side); for kind=local vaults this is typically empty
// since the SSE-mirror flow doesn't track folder activity.
func (c *PouchClient) Heartbeat(ctx context.Context, lastDropID string, totalDrops int64, paths []HeartbeatPath) error {
	in := map[string]any{
		"last_drop_id": lastDropID,
		"total_drops":  totalDrops,
	}
	if len(paths) > 0 {
		in["paths"] = paths
	}
	return c.post(ctx, "/api/vaults/heartbeat", in, nil)
}

// HeartbeatPath is one (path, count, stream?) tuple emitted with a
// heartbeat. Stream is set when this vault has the path declared
// with a stream binding (decision
// vault-declares-paths-cloud-reflects-channels) — the cloud
// reconciles channels from the heartbeat in addition to bumping
// rolling-window counters.
type HeartbeatPath struct {
	Path   string `json:"path"`
	Count  int64  `json:"count"`
	Stream string `json:"stream,omitempty"`
}

// PairResult is what /api/vaults/pair returns. All three secrets
// are shown ONCE — the caller is responsible for persisting them
// (typically by printing for the user to paste into vault.env).
type PairResult struct {
	VaultID    string `json:"vault_id"`
	VaultName  string `json:"vault_name"`
	ChannelID  string `json:"channel_id"`
	VaultKey   string `json:"vault_key"`
	HMACSecret string `json:"hmac_secret"`
	Mode       string `json:"mode"`
}

// PairInput is the body of POST /api/vaults/pair.
type PairInput struct {
	PairingKey   string `json:"pairing_key"`
	Kind         string `json:"kind"` // "local" | "git"
	MachineLabel string `json:"machine_label,omitempty"`
	Hostname     string `json:"hostname,omitempty"`
	Version      string `json:"version,omitempty"`
	PublicURL    string `json:"public_url,omitempty"`
}

// Pair claims a pairing key (no X-Vault-Key required — the pairing
// key IS the capability). Returns the long-lived credentials the
// vault needs for subsequent register / heartbeat / SSE calls.
func (c *PouchClient) Pair(ctx context.Context, in PairInput) (*PairResult, error) {
	body, err := json.Marshal(in)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST",
		c.BaseURL+"/api/vaults/pair", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", c.UserAgent)
	// No X-Vault-Key on the pair endpoint — it's the bootstrap call.

	cli := c.HTTPClient
	if cli == nil {
		cli = http.DefaultClient
	}
	resp, err := cli.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		buf, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("/api/vaults/pair: %d %s", resp.StatusCode, strings.TrimSpace(string(buf)))
	}
	var out PairResult
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

// DropInput is the body shape POST /api/items expects from a
// producer-mode vault. The cloud-side handler treats X-Vault-Key
// as auth and stamps source as "vault:<id>".
type DropInput struct {
	Label        string   `json:"label"`
	Body         string   `json:"body"`
	BodyEncoding string   `json:"body_encoding,omitempty"`
	MIME         string   `json:"mime,omitempty"`
	Stream       string   `json:"stream,omitempty"`
	Tags         []string `json:"tags,omitempty"`
	OriginalPath string   `json:"original_path,omitempty"`
}

// DropResult is the slim projection of POST /api/items we care
// about — just enough to record the drop_id in the state file.
type DropResult struct {
	ID string `json:"id"`
}

// PostDrop creates a drop in pouch on behalf of this vault's
// owner user. Auth is X-Vault-Key (Phase 5
// vault-producer-mode-and-local-only-git decision). Used by
// `pouch-vault sync` / `watch` after a new file is detected on
// a direction='watch' path.
func (c *PouchClient) PostDrop(ctx context.Context, in DropInput) (*DropResult, error) {
	var out DropResult
	if err := c.post(ctx, "/api/items", in, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// post is the shared codepath. Marshal, set headers, send, decode
// (when out != nil), surface non-2xx as an error.
func (c *PouchClient) post(ctx context.Context, path string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST",
		c.BaseURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", c.UserAgent)
	req.Header.Set("X-Vault-Key", c.APIKey)

	cli := c.HTTPClient
	if cli == nil {
		cli = http.DefaultClient
	}
	resp, err := cli.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		buf, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("%s: %d %s", path, resp.StatusCode, strings.TrimSpace(string(buf)))
	}
	if out != nil && resp.ContentLength != 0 {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil && err != io.EOF {
			return err
		}
	}
	return nil
}
