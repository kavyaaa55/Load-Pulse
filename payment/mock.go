package payment

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	goredis "github.com/redis/go-redis/v9"

	"github.com/livesale/live-sale/db"
	"github.com/livesale/live-sale/models"
)

// PayRequest is the expected JSON body for POST /pay.
type PayRequest struct {
	UserID    string `json:"user_id"    binding:"required"`
	ProductID string `json:"product_id" binding:"required"`
}

// PayHandler returns a Gin handler for POST /pay.
// It simulates an 80 % payment success rate.
func PayHandler(rdb *goredis.Client, database *db.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		var req PayRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "user_id and product_id are required"})
			return
		}

		ctx := context.Background()
		slotKey := fmt.Sprintf("slot:%s", req.UserID)
		boughtKey := fmt.Sprintf("bought:%s", req.UserID)
		invKey := fmt.Sprintf("inv:%s", req.ProductID)
		priceKey := fmt.Sprintf("price:%s", req.ProductID)

		// Verify the reservation slot is still valid.
		exists, err := rdb.Exists(ctx, slotKey).Result()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "redis error"})
			return
		}
		if exists == 0 {
			c.JSON(http.StatusGone, gin.H{"status": "SLOT_EXPIRED"})
			return
		}

		// Simulate payment outcome: 80 % success.
		if rand.Float64() < 0.80 {
			// ── Payment succeeded ────────────────────────────────────────────
			pipe := rdb.Pipeline()
			pipe.Set(ctx, boughtKey, 1, 0)
			pipe.Del(ctx, slotKey)
			if _, err := pipe.Exec(ctx); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "redis error"})
				return
			}

			// Fetch price for the order record.
			priceStr, _ := rdb.Get(ctx, priceKey).Result()
			price, _ := strconv.ParseFloat(priceStr, 64)

			// Persist the order asynchronously so the HTTP response is fast.
			go func() {
				order := models.Order{
					UserID:    req.UserID,
					ProductID: req.ProductID,
					Price:     price,
					Timestamp: time.Now(),
					Status:    "confirmed",
				}
				if err := database.InsertOrder(order); err != nil {
					log.Printf("payment: insert order failed for user %s: %v", req.UserID, err)
				}
			}()

			c.JSON(http.StatusOK, gin.H{"status": "ORDER_CONFIRMED"})

		} else {
			// ── Payment failed — return the inventory unit ───────────────────
			pipe := rdb.Pipeline()
			pipe.Incr(ctx, invKey)
			pipe.Del(ctx, slotKey)
			if _, err := pipe.Exec(ctx); err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "redis error"})
				return
			}

			c.JSON(http.StatusPaymentRequired, gin.H{"status": "PAYMENT_FAILED"})
		}
	}
}
