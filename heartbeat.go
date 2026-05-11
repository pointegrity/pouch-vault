package main

import (
	"context"
	"log"
	"time"
)

// runHeartbeats reports the vault's local stats to pouch every
// `interval`. Returns when ctx is cancelled. First tick fires
// immediately so the dashboard updates without waiting an interval.
//
// declaredPaths is the vault's VAULT_PATHS config — included on
// every heartbeat so the cloud's channel reconciliation stays in
// sync without needing a separate "re-register" cadence. Count
// stays 0 for kind=local since this binary doesn't track folder
// activity (the SSE-mirror flow handles drops at the channel
// level); the stream field on each entry is what makes
// reconciliation fire on the cloud side.
func runHeartbeats(ctx context.Context, client *PouchClient, store *Store, interval time.Duration, declaredPaths []ConfigPath) {
	if interval <= 0 {
		interval = 30 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()

	hbPaths := make([]HeartbeatPath, 0, len(declaredPaths))
	for _, p := range declaredPaths {
		hbPaths = append(hbPaths, HeartbeatPath{
			Path:   p.Path,
			Stream: p.Stream,
			// Count stays 0 — kind=local doesn't watch folders.
			// kind=git's heartbeat (different binary) is where
			// count is load-bearing.
		})
	}

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
		if err := client.Heartbeat(hbCtx, lastID, count, hbPaths); err != nil {
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
