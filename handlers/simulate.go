package handlers

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	goredis "github.com/redis/go-redis/v9"
)

const (
	simSlotTTL  = "120"
	simSlotSecs = 120 * time.Second
)

// simRunning prevents two simulations running at the same time.
var simRunning atomic.Bool

// SimulateHandler runs a full buy+pay simulation entirely inside Go goroutines.
// Results are streamed to all connected WebSocket clients in real time.
//
// POST /simulate/start
// Body: { "product_id": "iphone15", "vus": 10000 }
func SimulateHandler(rdb *goredis.Client, hub *Hub) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req struct {
			ProductID string `json:"product_id" binding:"required"`
			VUs       int    `json:"vus"`
		}
		if err := c.ShouldBindJSON(&req); err != nil || req.ProductID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "product_id required"})
			return
		}
		if req.VUs <= 0 || req.VUs > 10000 {
			req.VUs = 10000
		}

		if !simRunning.CompareAndSwap(false, true) {
			c.JSON(http.StatusConflict, gin.H{"error": "simulation already running"})
			return
		}

		c.JSON(http.StatusOK, gin.H{"status": "started", "vus": req.VUs})

		// Run simulation in background — results stream via WebSocket.
		go runSimulation(req.ProductID, req.VUs, rdb, hub)
	}
}

// runSimulation spawns VUs goroutines, each acting as one virtual user.
func runSimulation(productID string, vus int, rdb *goredis.Client, hub *Hub) {
	defer simRunning.Store(false)

	epoch := time.Now().UnixNano()
	hub.BroadcastSimEvent("sim_start", vus)

	var wg sync.WaitGroup

	// Stagger launches slightly so we don't hammer Redis all at once.
	// 10,000 goroutines in ~500ms = realistic burst.
	batchSize := 100
	batchDelay := 5 * time.Millisecond

	for i := 0; i < vus; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			userID := fmt.Sprintf("sim_%d_user_%d", epoch, idx)
			runVirtualUser(productID, userID, idx, rdb, hub)
		}(i)

		// Small stagger between batches
		if i > 0 && i%batchSize == 0 {
			time.Sleep(batchDelay)
		}
	}

	wg.Wait()
	hub.BroadcastSimEvent("sim_done", vus)
}

// runVirtualUser simulates one user: buy → pay → done.
func runVirtualUser(productID, userID string, idx int, rdb *goredis.Client, hub *Hub) {
	ctx := context.Background()

	invKey    := fmt.Sprintf("inv:%s", productID)
	queueKey  := fmt.Sprintf("queue:%s", productID)
	slotsKey  := fmt.Sprintf("slots:%s", productID)
	priceKey  := fmt.Sprintf("price:%s", productID)
	slotKey   := fmt.Sprintf("slot:%s", userID)
	boughtKey := fmt.Sprintf("bought:%s", userID)

	// ── Step 1: gate check + enqueue ─────────────────────────────────────────
	hub.BroadcastUserEvent(idx, "waiting")

	invCount, err := rdb.Get(ctx, invKey).Int()
	if err != nil || invCount <= 0 {
		hub.BroadcastUserEvent(idx, "lost")
		return
	}

	queueLen, _ := rdb.LLen(ctx, queueKey).Result()
	activeSlots, _ := rdb.SCard(ctx, slotsKey).Result()
	if queueLen+activeSlots >= int64(invCount) {
		hub.BroadcastUserEvent(idx, "lost")
		return
	}

	_, err = rdb.RPush(ctx, queueKey, userID).Result()
	if err != nil {
		hub.BroadcastUserEvent(idx, "lost")
		return
	}

	// ── Step 2: wait for queue worker to assign slot ──────────────────────────
	// Poll for slot:userID to appear (worker sets it via Lua script).
	deadline := time.Now().Add(30 * time.Second) // generous timeout
	slotAssigned := false
	for time.Now().Before(deadline) {
		exists, err := rdb.Exists(ctx, slotKey).Result()
		if err == nil && exists == 1 {
			slotAssigned = true
			break
		}
		bought, _ := rdb.Exists(ctx, boughtKey).Result()
		if bought == 1 {
			hub.BroadcastUserEvent(idx, "won")
			return
		}
		time.Sleep(20 * time.Millisecond) // less aggressive polling
	}

	if !slotAssigned {
		// Worker said SOLD_OUT for this user — queue was drained before our turn
		hub.BroadcastUserEvent(idx, "lost")
		return
	}

	// ── Step 3: simulate payment (80% success) ────────────────────────────────
	// Small random delay — simulate user "confirming" payment
	time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)

	// Check slot still valid
	exists, _ := rdb.Exists(ctx, slotKey).Result()
	if exists == 0 {
		hub.BroadcastUserEvent(idx, "expired")
		return
	}

	if rand.Float64() < 0.80 {
		// Payment success
		pipe := rdb.Pipeline()
		pipe.Set(ctx, boughtKey, 1, 0)
		pipe.Del(ctx, slotKey)
		pipe.SRem(ctx, slotsKey, userID)
		if _, err := pipe.Exec(ctx); err != nil {
			hub.BroadcastUserEvent(idx, "lost")
			return
		}

		// Read price for logging
		priceStr, _ := rdb.Get(ctx, priceKey).Result()
		price, _ := strconv.ParseFloat(priceStr, 64)
		_ = price

		hub.BroadcastUserEvent(idx, "won")
	} else {
		// Payment failed — return inventory
		pipe := rdb.Pipeline()
		pipe.Incr(ctx, invKey)
		pipe.Del(ctx, slotKey)
		pipe.SRem(ctx, slotsKey, userID)
		pipe.Exec(ctx)

		hub.BroadcastUserEvent(idx, "lost")
	}
}
