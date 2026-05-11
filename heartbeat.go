package main

import (
	"context"
	"log"
	"time"
)

// runHeartbeats reports the vault's local stats to pouch every
// `interval`. Returns when ctx is cancelled. First tick fires
// immediately so the dashboard updates without waiting an interval.
func runHeartbeats(ctx context.Context, client *PouchClient, store *Store, interval time.Duration) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()

	tick := func() {
		count, lastID, err := store.Stats(ctx)
		if err != nil {
			log.Printf("heartbeat: store stats: %v", err)
			return
		}
		// 5s timeout on the heartbeat itself — don't let a slow
		// pouch wedge the loop.
		hbCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		// Paths stay nil for kind=local — this vault doesn't watch
		// folders; the SSE-mirror flow tracks drops at the channel
		// level. pouch-vault-git's heartbeat is where per-path counts
		// matter. The cloud's rolling-window code becomes a no-op when
		// paths is empty (just bumps last_seen_at).
		if err := client.Heartbeat(hbCtx, lastID, count, nil); err != nil {
			log.Printf("heartbeat: %v", err)
			status.MarkHeartbeatError(err.Error())
			return
		}
		status.MarkHeartbeat()
	}

	tick() // immediate first beat
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			tick()
		}
	}
}
