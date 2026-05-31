package handlers

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	goredis "github.com/redis/go-redis/v9"
)

// InventoryHandler returns a Gin handler for GET /inventory/:productID.
func InventoryHandler(rdb *goredis.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		productID := c.Param("productID")
		ctx := context.Background()

		invKey := fmt.Sprintf("inv:%s", productID)
		priceKey := fmt.Sprintf("price:%s", productID)

		// Fetch both keys in a single pipeline to reduce round-trips.
		pipe := rdb.Pipeline()
		invCmd := pipe.Get(ctx, invKey)
		priceCmd := pipe.Get(ctx, priceKey)
		if _, err := pipe.Exec(ctx); err != nil && err != goredis.Nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "redis error"})
			return
		}

		countStr, err := invCmd.Result()
		if err == goredis.Nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "product not found"})
			return
		}
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "redis error"})
			return
		}

		count, _ := strconv.Atoi(countStr)

		priceStr, _ := priceCmd.Result()
		price, _ := strconv.ParseFloat(priceStr, 64)

		c.JSON(http.StatusOK, gin.H{
			"product_id": productID,
			"count":      count,
			"price":      price,
		})
	}
}

// QueuePositionHandler returns a Gin handler for GET /queue/position/:userID/:productID.
func QueuePositionHandler(rdb *goredis.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		userID := c.Param("userID")
		productID := c.Param("productID")
		ctx := context.Background()

		queueKey := fmt.Sprintf("queue:%s", productID)

		// LPOS returns the 0-based index of the first occurrence of the element.
		pos, err := rdb.LPos(ctx, queueKey, userID, goredis.LPosArgs{}).Result()
		if err == goredis.Nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "not in queue"})
			return
		}
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "redis error"})
			return
		}

		// Convert 0-based index to 1-based position.
		c.JSON(http.StatusOK, gin.H{"position": pos + 1})
	}
}
