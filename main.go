package main

import (
	"context"
	"fmt"
	"log"

	"github.com/gin-gonic/gin"

	"github.com/livesale/live-sale/db"
	"github.com/livesale/live-sale/handlers"
	"github.com/livesale/live-sale/payment"
	"github.com/livesale/live-sale/queue"
	redisclient "github.com/livesale/live-sale/redis"
)

// product describes a product seeded into Redis on startup.
type product struct {
	id    string
	units int
	price float64
}

// catalog is the initial product catalogue.
var catalog = []product{
	{"iphone15", 100, 999},
	{"macbook", 50, 1999},
	{"airpods", 200, 199},
	{"ipad", 75, 799},
	{"applewatch", 150, 399},
}

func main() {
	// ── Redis ────────────────────────────────────────────────────────────────
	if err := redisclient.Connect(); err != nil {
		log.Fatalf("failed to connect to Redis: %v", err)
	}
	redisclient.LoadScripts()
	log.Println("Redis connected")

	// ── Postgres ─────────────────────────────────────────────────────────────
	database, err := db.Connect()
	if err != nil {
		log.Fatalf("failed to connect to Postgres: %v", err)
	}
	log.Println("Postgres connected")

	// ── Seed products (SET NX — only if key does not exist) ──────────────────
	ctx := context.Background()
	for _, p := range catalog {
		invKey := fmt.Sprintf("inv:%s", p.id)
		priceKey := fmt.Sprintf("price:%s", p.id)

		// Only seed when the inventory key is absent.
		set, err := redisclient.RDB.SetNX(ctx, invKey, p.units, 0).Result()
		if err != nil {
			log.Fatalf("seed: SetNX inv failed for %s: %v", p.id, err)
		}
		if set {
			log.Printf("seed: %s — %d units @ $%.0f", p.id, p.units, p.price)
		}

		// Always keep price in sync (idempotent).
		if err := redisclient.RDB.SetNX(ctx, priceKey, fmt.Sprintf("%.2f", p.price), 0).Err(); err != nil {
			log.Fatalf("seed: SetNX price failed for %s: %v", p.id, err)
		}
	}

	// ── WebSocket hub ─────────────────────────────────────────────────────────
	hub := handlers.NewHub()
	go hub.Run()

	// ── Queue workers (one per product) ──────────────────────────────────────
	workerCtx := context.Background()
	for _, p := range catalog {
		go queue.StartWorker(workerCtx, p.id, redisclient.RDB, hub)
	}

	// ── HTTP router ───────────────────────────────────────────────────────────
	r := gin.Default()

	// Serve the demo UI
	r.Static("/static", "./static")
	r.GET("/", func(c *gin.Context) {
		c.File("./static/index.html")
	})

	r.POST("/buy", handlers.BuyHandler(redisclient.RDB))
	r.GET("/inventory/:productID", handlers.InventoryHandler(redisclient.RDB))
	r.GET("/queue/position/:userID/:productID", handlers.QueuePositionHandler(redisclient.RDB))
	r.GET("/ws", hub.ServeWS)
	r.POST("/pay", payment.PayHandler(redisclient.RDB, database))
	r.POST("/simulate/start", handlers.SimulateHandler(redisclient.RDB, hub))

	log.Println("Listening on :8080")
	if err := r.Run(":8080"); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
