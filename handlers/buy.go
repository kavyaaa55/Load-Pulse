package handlers

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	goredis "github.com/redis/go-redis/v9"

	redisclient "github.com/livesale/live-sale/redis"
)

const (
	buySlotTTL    = "120"          // seconds — must match queue/worker.go SlotTTLSecs
	buySemSize    = 256            // max concurrent /buy requests in flight
)

// buySem caps how many /buy goroutines can be inside the Lua call at once.
// When full we return 503 immediately instead of queuing behind a pool timeout.
var buySem = make(chan struct{}, buySemSize)

// BuyRequest is the expected JSON body for POST /buy.
type BuyRequest struct {
	UserID    string `json:"user_id"    binding:"required"`
	ProductID string `json:"product_id" binding:"required"`
}

// BuyHandler returns a Gin handler for POST /buy.
// One atomic Lua call replaces the previous 4-round-trip sequence, eliminating
// both the TOCTOU race and pool exhaustion under load.
func BuyHandler(rdb *goredis.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		// ── Semaphore: shed load fast instead of timing out ──────────────────
		select {
		case buySem <- struct{}{}:
			defer func() { <-buySem }()
		default:
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "server busy, try again"})
			return
		}

		var req BuyRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "user_id and product_id are required"})
			return
		}

		invKey    := fmt.Sprintf("inv:%s", req.ProductID)
		slotKey   := fmt.Sprintf("slot:%s", req.UserID)
		boughtKey := fmt.Sprintf("bought:%s", req.UserID)
		slotsKey  := fmt.Sprintf("slots:%s", req.ProductID)
		expiryKey := fmt.Sprintf("expiry:%s", req.ProductID)

		// expiry score = now + TTL  (same value the reaper reads)
		expiryScore := fmt.Sprintf("%d", time.Now().Unix()+120)

		ctx := context.Background()
		result, err := redisclient.AtomicBuyScript.Run(
			ctx,
			rdb,
			[]string{invKey, slotKey, boughtKey, slotsKey, expiryKey},
			buySlotTTL,
			expiryScore,
			req.UserID,
		).Text()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "redis error"})
			return
		}

		switch result {
		case "OK":
			// Slot assigned atomically — no queue worker needed for /buy path.
			// Return position 1 (immediate slot, not a queue position).
			c.JSON(http.StatusOK, gin.H{
				"status":   "queued",
				"position": 1,
			})
		case "SOLD_OUT":
			c.JSON(http.StatusConflict, gin.H{"error": "sold out"})
		case "ALREADY_BOUGHT":
			c.JSON(http.StatusConflict, gin.H{"error": "already purchased"})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "unexpected result"})
		}
	}
}

// clientIP extracts the real client IP, preferring X-Forwarded-For.
func clientIP(c *gin.Context) string {
	if xff := c.GetHeader("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		return strings.TrimSpace(parts[0])
	}
	addr := c.Request.RemoteAddr
	if idx := strings.LastIndex(addr, ":"); idx != -1 {
		return addr[:idx]
	}
	return addr
}
