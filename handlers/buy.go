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
// It enforces per-user and per-IP rate limiting, then enqueues the request.
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

		// ── Per-IP rate limit ────────────────────────────────────────────────
		ip := clientIP(c)
		ipRateKey := fmt.Sprintf("ratelimit:ip:%s", ip)
		ipSet, err := rdb.SetNX(ctx, ipRateKey, 1, 60*time.Second).Result()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "redis error"})
			return
		}
		if !ipSet {
			c.JSON(http.StatusTooManyRequests, gin.H{"error": "rate limited (ip)"})
			return
		}

		// ── Verify product exists ────────────────────────────────────────────
		invKey := fmt.Sprintf("inv:%s", req.ProductID)
		exists, err := rdb.Exists(ctx, invKey).Result()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "redis error"})
			return
		}
		if exists == 0 {
			c.JSON(http.StatusNotFound, gin.H{"error": "product not found"})
			return
		}

		// ── Enqueue ──────────────────────────────────────────────────────────
		queueKey := fmt.Sprintf("queue:%s", req.ProductID)
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
