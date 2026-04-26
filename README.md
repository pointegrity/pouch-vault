# pouch-anchor

Headless local relay daemon for [pouch](https://pouch.pointegrity.com).

Receives every drop from your pouch instance over HTTPS, persists it
to a local SQLite database, and reports back periodically so the
SaaS knows your archive is healthy. The point: a personal,
self-controlled mirror of everything pouch holds for you, without
trusting pouch's storage to be eternal.

## What it does

```
[pouch SaaS] ── HTTPS POST /hook ──▶ [pouch-anchor] ──▶ /var/lib/pouch-anchor/drops.db
     ▲                                       │
     └──── HTTPS heartbeat every 30s ────────┘
           "I have N drops, last id was X"
```

- **Receive** — webhook deliveries from pouch, signed with the HMAC
  secret you minted at provisioning. Bad signature → 401, retried
  delivery → silently deduped via `X-Pouch-Delivery`.
- **Persist** — one row per drop in a local SQLite DB. Pair with
  [litestream](https://litestream.io) for offsite backup at
  seconds-of-lag RPO.
- **Heartbeat** — anchor reports back to pouch every 30 s with
  `last_drop_id`, `total_drops`, `hostname`, and version. The pouch
  SaaS uses this to populate its replication-status UI.

## Provisioning (one-time)

On the **pouch server** (admin shell):

```bash
pouch anchor create --owner <user-id> --name <anchor-name>
```

Output is shown **once**. Save the three values you'll need on the
anchor host:

```
POUCH_URL          https://pouch.pointegrity.com
POUCH_ANCHOR_KEY   pk_...                        (auth, long-lived)
POUCH_HMAC_SECRET  ...                           (delivery signature)
```

## Install

Pre-built binaries for **linux/amd64**, **linux/arm64**, **darwin/amd64**,
**darwin/arm64**, and **windows/amd64** are attached to every
[GitHub release](https://github.com/pointegrity/pouch-anchor/releases).
A Docker image is published as part of the same release flow.

The binary is **pure Go** (modernc/sqlite) — no CGO, no glibc/musl
worries, statically linked. Copy it onto the box and run.

### macOS (Apple Silicon)

```bash
curl -fL -o pouch-anchor https://github.com/pointegrity/pouch-anchor/releases/latest/download/pouch-anchor-darwin-arm64
chmod +x pouch-anchor
sudo mv pouch-anchor /usr/local/bin/
```

### Linux (amd64)

```bash
curl -fL -o pouch-anchor https://github.com/pointegrity/pouch-anchor/releases/latest/download/pouch-anchor-linux-amd64
chmod +x pouch-anchor
sudo install -m 755 pouch-anchor /usr/local/bin/
```

### From source (any platform with Go 1.22+)

```bash
git clone https://github.com/pointegrity/pouch-anchor
cd pouch-anchor
go build -o pouch-anchor .
```

## Run

The binary is a foreground process — handles SIGTERM, logs to stderr,
doesn't fork. Pick whichever supervisor matches your OS:

### Foreground (any OS)

Simplest. Good for testing or "leave it running on my home server
for a few hours":

```bash
POUCH_URL=https://pouch.pointegrity.com \
POUCH_ANCHOR_KEY=pk_... \
POUCH_HMAC_SECRET=... \
POUCH_PUBLIC_URL=https://anchor.example/hook \
ANCHOR_DB=./drops.db \
pouch-anchor
```

### systemd (Linux servers, Pi, NAS)

```bash
sudo install -d -m 700 -o pouch:pouch /etc/pouch
sudo install -m 600 -o pouch:pouch examples/pouch-anchor.env /etc/pouch/anchor.env
$EDITOR /etc/pouch/anchor.env
sudo install -d -m 755 -o pouch:pouch /var/lib/pouch-anchor

sudo install -m 644 examples/pouch-anchor.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now pouch-anchor
sudo journalctl -u pouch-anchor -f
```

### launchd (macOS, always-on Mac)

```bash
cp examples/com.pointegrity.pouch-anchor.plist ~/Library/LaunchAgents/
$EDITOR ~/Library/LaunchAgents/com.pointegrity.pouch-anchor.plist  # fill in env
launchctl load ~/Library/LaunchAgents/com.pointegrity.pouch-anchor.plist
log stream --predicate 'process == "pouch-anchor"'
```

### Docker (any OS that runs Docker)

```bash
docker run -d --name pouch-anchor \
  -p 7780:7780 \
  -v pouch-anchor-data:/data \
  -e POUCH_URL=https://pouch.pointegrity.com \
  -e POUCH_ANCHOR_KEY=pk_... \
  -e POUCH_HMAC_SECRET=... \
  -e POUCH_PUBLIC_URL=https://anchor.example/hook \
  ghcr.io/pointegrity/pouch-anchor:latest
```

### Windows

There's no native Windows-service shim yet. Run from a terminal, or
register a Scheduled Task with trigger "At startup" pointing at
`pouch-anchor.exe` with the env vars set in the action's user
context. Native `sc create` integration is on the Shape B roadmap.

After any of these, you should see in the logs:

```
pouch-anchor 0.1.0 listening on 127.0.0.1:7780, db=/var/lib/pouch-anchor/drops.db, pouch=https://pouch.pointegrity.com
registered as anc-... (name=jy-laptop, public_url=https://anchor.example/hook)
```

## Verify it works

Drop something into your pouch (CLI, SPA, ingress key — whichever).
Within a few seconds:

```bash
sqlite3 /var/lib/pouch-anchor/drops.db \
  "SELECT received_at, drop_id, label FROM drops ORDER BY received_at DESC LIMIT 5"
```

In the pouch admin shell:

```bash
pouch anchor ls --owner <user-id>
# DROPS column climbs as deliveries land.
```

## Schema (local)

```sql
CREATE TABLE drops (
  delivery_id  TEXT PRIMARY KEY,    -- X-Pouch-Delivery (idempotency)
  drop_id      TEXT NOT NULL,       -- pouch's itm-...
  pouch_user   TEXT NOT NULL,
  stream       TEXT NOT NULL,
  label        TEXT,
  body         TEXT,
  tags         TEXT,                -- JSON array
  mime         TEXT,
  source       TEXT,
  created_at   DATETIME NOT NULL,   -- in pouch's timezone
  received_at  DATETIME NOT NULL    -- when we wrote it
);
```

Plain text body for now. Binary drops (images, audio, PDFs) are on
the roadmap; the schema will gain a `body_blob` BLOB column or move
binaries onto disk, and Git-LFS-style selective fetch becomes useful
on the receiver side. See pouch issue tracker.

## Configuration reference

| env / flag | meaning | default |
|---|---|---|
| `POUCH_URL` / `--pouch-url` | pouch SaaS base URL | (required) |
| `POUCH_ANCHOR_KEY` / `--anchor-key` | API key from `pouch anchor create` | (required) |
| `POUCH_HMAC_SECRET` / `--hmac-secret` | delivery signature secret | (required) |
| `POUCH_PUBLIC_URL` / `--public-url` | where pouch reaches us | (required) |
| `ANCHOR_DB` / `--db` | sqlite database path | `drops.db` |
| `ANCHOR_LISTEN` / `--addr` | listen address | `:7780` |
| `ANCHOR_NAME` / `--name` | anchor name (heartbeat label) | `$HOSTNAME` |
| `--heartbeat` | heartbeat interval | `30s` |

CLI flags override env. Useful for local dev:

```bash
POUCH_ANCHOR_KEY=pk_xxx \
POUCH_HMAC_SECRET=abc \
go run . -pouch-url http://localhost:8080 -public-url http://127.0.0.1:7780/hook -addr :7780
```

## Rotating credentials

If the API key or HMAC secret leaks, on the pouch admin shell:

```bash
pouch anchor rm <anchor-id> --owner <user-id>     # nukes everything
pouch anchor create --owner <user-id> --name ...  # mint replacement
```

Then update `/etc/pouch/anchor.env` on the anchor host and
`systemctl restart pouch-anchor`.

## License

ISC.
