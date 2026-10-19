package queue

import (
	"context"
	"fmt"
	"log"
	"time"

	goredis "github.com/redis/go-redis/v9"

	redisclient "github.com/livesale/live-sale/redis"
)

const (
	reaperInterval = 1 * time.Second // how often to scan for expired slots
	reaperBatch    = "100"           // max slots to reap per product per tick
)

// StartReaper runs a single goroutine that scans every product's expiry sorted
// set once per second. For each slot whose expiry score <= now it atomically:
//   - checks the slot key still exists (skips if /pay already cleaned it up)
//   - DELs the slot key
//   - INCRs inventory back
//   - ZREMs from expiry:<productID>
//   - SREMs from slots:<productID>
//
// This is the sole recovery path for abandoned reservations. It runs until the
// context is cancelled (i.e. the whole server shuts down).
func StartReaper(ctx context.Context, productIDs []string, rdb *goredis.Client) {
	log.Println("reaper: started")
	ticker := time.NewTicker(reaperInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("reaper: context cancelled, stopping")
			return
		case <-ticker.C:
			reapAll(ctx, productIDs, rdb)
		}
	}
}

// reapAll iterates every product and calls the Lua reaper script.
func reapAll(ctx context.Context, productIDs []string, rdb *goredis.Client) {
	now := fmt.Sprintf("%d", time.Now().Unix())

	for _, productID := range productIDs {
		expiryKey := fmt.Sprintf("expiry:%s", productID)
		invKey    := fmt.Sprintf("inv:%s", productID)
		slotsKey  := fmt.Sprintf("slots:%s", productID)

		n, err := redisclient.ReaperScript.Run(
			ctx,
			rdb,
			[]string{expiryKey, invKey, slotsKey},
			now,
			reaperBatch,
		).Int()
		if err != nil && err != goredis.Nil {
			log.Printf("reaper[%s]: script error: %v", productID, err)
			continue
		}
		if n > 0 {
			log.Printf("reaper[%s]: recovered %d expired slot(s) — inventory returned", productID, n)
		}
	}
}
