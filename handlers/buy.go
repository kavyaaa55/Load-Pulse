package handlers

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	goredis "github.com/redis/go-redis/v9"
)

// BuyRequest is the expected JSON body for POST /buy.
type BuyRequest struct {
	UserID    string `json:"user_id"    binding:"required"`
	ProductID string `json:"product_id" binding:"required"`
}

// BuyHandler returns a Gin handler for POST /buy.
// It enforces per-user rate limiting, gate check, then enqueues the request.
func BuyHandler(rdb *goredis.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req BuyRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "user_id and product_id are required"})
			return
		}

		ctx := context.Background()

		// ── Per-user rate limit ──────────────────────────────────────────────
		userRateKey := fmt.Sprintf("ratelimit:%s", req.UserID)
		set, err := rdb.SetNX(ctx, userRateKey, 1, 60*time.Second).Result()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "redis error"})
			return
		}
		if !set {
			c.JSON(http.StatusTooManyRequests, gin.H{"error": "rate limited"})
			return
		}

		// ── Per-IP rate limit (disabled for load testing — all k6 traffic same IP) ──
		// ip := clientIP(c)
		// ipRateKey := fmt.Sprintf("ratelimit:ip:%s", ip)
		// ipSet, err := rdb.SetNX(ctx, ipRateKey, 1, 60*time.Second).Result()
		// if err != nil {
		// 	c.JSON(http.StatusInternalServerError, gin.H{"error": "redis error"})
		// 	return
		// }
		// if !ipSet {
		// 	c.JSON(http.StatusTooManyRequests, gin.H{"error": "rate limited (ip)"})
		// 	return
		// }

		// ── Verify product exists + get inventory ────────────────────────────
		invKey := fmt.Sprintf("inv:%s", req.ProductID)
		invCount, err := rdb.Get(ctx, invKey).Int()
		if err == goredis.Nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "product not found"})
			return
		}
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "redis error"})
			return
		}

		// ── Sold out fast path ───────────────────────────────────────────────
		if invCount <= 0 {
			c.JSON(http.StatusConflict, gin.H{"error": "sold out"})
			return
		}

		// ── Gate check: queue + active slots >= inventory → reject ───────────
		// This prevents people from joining a queue when all items are already
		// spoken for (either being paid for or waiting in queue).
		queueKey := fmt.Sprintf("queue:%s", req.ProductID)
		slotsKey := fmt.Sprintf("slots:%s", req.ProductID)

		queueLen, err := rdb.LLen(ctx, queueKey).Result()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "redis error"})
			return
		}

		activeSlots, err := rdb.SCard(ctx, slotsKey).Result()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "redis error"})
			return
		}

		if queueLen+activeSlots >= int64(invCount) {
			c.JSON(http.StatusConflict, gin.H{"error": "sold out"})
			return
		}

		// ── Enqueue ──────────────────────────────────────────────────────────
		pos, err := rdb.RPush(ctx, queueKey, req.UserID).Result()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "redis error"})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"status":   "queued",
			"position": pos,
		})
	}
}

// clientIP extracts the real client IP, preferring X-Forwarded-For.
func clientIP(c *gin.Context) string {
	if xff := c.GetHeader("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		return strings.TrimSpace(parts[0])
	}
	// RemoteAddr is "host:port" — strip the port.
	addr := c.Request.RemoteAddr
	if idx := strings.LastIndex(addr, ":"); idx != -1 {
		return addr[:idx]
	}
	return addr
}