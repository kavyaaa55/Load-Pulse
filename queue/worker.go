package queue

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/livesale/live-sale/handlers"
	redisclient "github.com/livesale/live-sale/redis"
)

const (
	slotTTL      = "120"            // seconds — reservation window after atomic buy
	SlotTTLSecs  = 120              // same value as int, used by reaper for ZADD score
	pollInterval = 1 * time.Millisecond
	priceHike    = 1.20             // 20 % increase when stock drops to ≤ 10
	lowStockMark = 10
)

// StartWorker processes the buy queue for a single product.
// It runs until the product is sold out or the context is cancelled.
func StartWorker(ctx context.Context, productID string, rdb *goredis.Client, hub *handlers.Hub) {
	queueKey  := fmt.Sprintf("queue:%s", productID)
	invKey    := fmt.Sprintf("inv:%s", productID)
	priceKey  := fmt.Sprintf("price:%s", productID)
	slotsKey  := fmt.Sprintf("slots:%s", productID)  // SET — gate check
	expiryKey := fmt.Sprintf("expiry:%s", productID) // ZSET — reaper tracks TTLs

	log.Printf("worker[%s]: started", productID)

	for {
		select {
		case <-ctx.Done():
			log.Printf("worker[%s]: context cancelled, stopping", productID)
			return
		default:
		}

		// LPOP — non-blocking dequeue.
		userID, err := rdb.LPop(ctx, queueKey).Result()
		if err == goredis.Nil {
			// Queue is empty; back off briefly.
			time.Sleep(pollInterval)
			continue
		}
		if err != nil {
			log.Printf("worker[%s]: LPOP error: %v", productID, err)
			time.Sleep(pollInterval)
			continue
		}

		slotKey   := fmt.Sprintf("slot:%s", userID)
		boughtKey := fmt.Sprintf("bought:%s", userID)

		// Run the atomic Lua script — 5 KEYS + 3 ARGV.
		// Script atomically: EXISTS bought → GET inv → DECR → SET slot →
		// EXPIRE slot → ZADD expiry → SADD slots → return result.
		expiryScore := fmt.Sprintf("%d", time.Now().Unix()+SlotTTLSecs)
		result, err := redisclient.AtomicBuyScript.Run(
			ctx,
			rdb,
			[]string{invKey, slotKey, boughtKey, slotsKey, expiryKey},
			slotTTL,
			expiryScore,
			userID,
		).Text()
		if err != nil {
			log.Printf("worker[%s]: lua error for user %s: %v", productID, userID, err)
			continue
		}

		switch result {
		case "OK":
			// Slot assigned atomically inside the Lua script —
			// ZADD expiry and SADD slots already done, nothing extra needed here.

			// Read the new inventory count.
			countStr, err := rdb.Get(ctx, invKey).Result()
			if err != nil {
				log.Printf("worker[%s]: GET inv error: %v", productID, err)
				continue
			}
			count, _ := strconv.Atoi(countStr)

			// Read current price.
			priceStr, _ := rdb.Get(ctx, priceKey).Result()
			price, _ := strconv.ParseFloat(priceStr, 64)

			// Dynamic pricing: surge when stock is critically low.
			if count <= lowStockMark {
				newPrice := price * priceHike
				if setErr := rdb.Set(ctx, priceKey, fmt.Sprintf("%.2f", newPrice), 0).Err(); setErr != nil {
					log.Printf("worker[%s]: SET price error: %v", productID, setErr)
				} else {
					price = newPrice
					log.Printf("worker[%s]: surge pricing activated — new price %.2f", productID, price)
				}
			}

			hub.BroadcastInventory(productID, count, price)
			log.Printf("worker[%s]: reserved slot for user %s, inventory=%d", productID, userID, count)

		case "SOLD_OUT":
		priceStr, _ := rdb.Get(ctx, priceKey).Result()
		price, _ := strconv.ParseFloat(priceStr, 64)
		hub.BroadcastInventory(productID, 0, price)
		log.Printf("worker[%s]: sold out, waiting for restocks", productID)
		time.Sleep(10 * time.Millisecond)  
		// return  

		case "ALREADY_BOUGHT":
			log.Printf("worker[%s]: user %s already bought, skipping", productID, userID)

		default:
			log.Printf("worker[%s]: unexpected lua result: %s", productID, result)
		}
	}
}