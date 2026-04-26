# pouch-anchor (and pouch CLI)

This repo ships **two** binaries that complement each other:

- `pouch-anchor` — local relay daemon. Receives every drop from your
  pouch over a single outbound HTTPS connection and mirrors it to a
  local SQLite archive. (See "What it does" below.)
- `pouch` — minimal client CLI. Send drops *to* your pouch from a
  shell: pipe / file / clipboard. Quick example:
  ```bash
  echo "first cli drop" | pouch put --label hello
  pouch put README.md --tag docs
  pouch put -c                   # from system clipboard
  ```
  See [pouch CLI](#pouch-cli-pouch-put) below.

Both binaries are pure-Go, statically linked, no CGO, cross-built on
GitHub Actions for Linux (amd64/arm64), macOS (Intel/Apple Silicon)
and Windows.

Connects to your pouch over a single outbound HTTPS link, receives
every drop as it happens, and persists it to a local SQLite archive.
The point: a personal, self-controlled mirror of everything pouch
holds for you, without trusting pouch's storage to be eternal — and
with no firewall opening, no port forwarding, no tunneling needed.

## What it does (default: pull mode)

```
[pouch SaaS]  ◀──── outbound HTTPS SSE stream ────  [pouch-anchor]  ──▶  drops.db
              ◀──── outbound HTTPS heartbeat ─────
                  every 30 s: "I have N drops"
```

**Both arrows go outward from your anchor box.** Pouch never tries to
reach in. That means it works behind NAT, CGNAT, captive Wi-Fi,
corporate proxies — anywhere outbound HTTPS works. Drop a binary on
a Mac mini at home, fill in three values, you're done.

- **Receive** — drops arrive over an SSE stream the anchor holds open
  to pouch. Each event is HMAC-signed with the secret you minted at
  provisioning; bad signature is rejected, replays are deduped.
- **Persist** — one row per drop in a local SQLite DB. Pair with
  [litestream](https://litestream.io) for continuous offsite backup
  at seconds-of-lag RPO.
- **Heartbeat** — anchor reports `last_drop_id`, `total_drops`,
  `hostname`, and version every 30 s; pouch's UI uses this to render
  the replication-status panel.
- **Reconnect-replay** — drops that pouch tried to deliver while the
  anchor was offline are queued; on reconnect, pouch re-fires them
  immediately so the archive catches up.

## Push mode (advanced)

If you happen to operate a publicly-reachable HTTPS endpoint (server
with static IP and a real domain, cloudflared tunnel, tailscale
funnel, nginx with Let's Encrypt), you can set `POUCH_PUBLIC_URL`
and pouch will POST drops to that URL instead. Same wire format,
same HMAC, same anchor binary — pull mode is just "no public URL,
use the SSE stream." Most users will not need this.

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

### One-time scaffold (recommended)

After installing the binary, on each anchor host run:

```bash
pouch-anchor init
```

This creates the OS-conventional config + data directories and drops
a stub config you fill in:

| OS | config file | database |
|---|---|---|
| Linux | `~/.config/pouch-anchor/anchor.env` | `~/.local/share/pouch-anchor/drops.db` |
| macOS | `~/Library/Application Support/pouch-anchor/anchor.env` | (same dir)/drops.db |
| Windows | `%AppData%\pouch-anchor\anchor.env` | (same dir)\drops.db |

Then edit the config file with the values from `pouch anchor create`
(see Provisioning above) and run `pouch-anchor` — no env vars needed,
it picks up the file automatically.

The daemon's config-file lookup chain:

1. `--config <path>` flag, or `$POUCH_ANCHOR_CONFIG`
2. `<user-config-dir>/pouch-anchor/anchor.env` (the `init`-scaffolded path)
3. `/etc/pouch/anchor.env` (system-wide, useful for systemd)

File values fill any env var **not already set** — so explicit env
or CLI flags always win.

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

## pouch CLI (`pouch put`)

The same release ships `pouch`, a tiny client for sending drops *to*
your pouch from any shell. Distinct from the anchor — the anchor
**receives** drops, the CLI **sends** them. Use both, neither, or one.

### Provisioning

On the **pouch server**, an admin runs:

```bash
pouch key create --owner <your-user-id> --label <a-name-you-pick>
```

The plaintext ingress key is shown **once**. Save it; you'll paste
it into the CLI's config file.

### Install + scaffold config

```bash
# Linux amd64 example — pick the right binary for your OS/arch
curl -fL -o pouch https://github.com/pointegrity/pouch-anchor/releases/latest/download/pouch-linux-amd64
chmod +x pouch
sudo install -m 755 pouch /usr/local/bin/

pouch init              # creates ~/.config/pouch/config.env
$EDITOR ~/.config/pouch/config.env   # paste POUCH_KEY
```

Config file format:

```
POUCH_URL=https://pouch.pointegrity.com
POUCH_KEY=pk_...
```

### Use it

```bash
# Pipe stdin (write — uses POUCH_KEY)
echo "quick note" | pouch put --label "review"
git diff | pouch put --label "WIP diff" --tag wip

# File
pouch put README.md
pouch put report.pdf --binary --mime application/pdf

# Clipboard (-c or --clipboard)
pouch put -c
pouch put -c --label "from clipboard"

# Tags, stream, TTL
pouch put notes.md --tag work --tag friday --stream kept
echo ephemeral | pouch put --ttl 1h

# Bare invocation acts as `pouch put` if stdin is piped
cat ~/.bash_history | tail -100 | pouch

# Read access (uses login token, not the ingress key)
pouch login --user you           # prompts for password, saves token
pouch ls                         # newest 25
pouch ls --stream kept --tag work --limit 50
pouch ls --query "release notes" --json | jq .
pouch get itm-...                # body to stdout
pouch get itm-... -o note.md     # body to file
pouch get itm-... --json         # whole record
```

Output is the new drop's `itm-...` id — pipe-friendly:

```bash
ID=$(echo "test" | pouch put)
echo "Created $ID"
```

### Binary input

The CLI **refuses** non-UTF-8 / NUL-containing input by default so an
accidental `cat image.jpg | pouch put` doesn't land mojibake. Pass
`--binary --mime <type>` when you really mean it:

```bash
pouch put image.png --binary --mime image/png
```

> Pouch's binary-roundtrip story is provisional until the
> binary-body-support work ships — re-reading a binary drop's body
> may not preserve bytes exactly. Text drops always roundtrip cleanly.

## License

ISC.
