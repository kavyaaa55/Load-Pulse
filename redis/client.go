package redis

import (
	"context"
	"fmt"
	"os"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// RDB is the shared Redis client used across the application.
var RDB *goredis.Client

// Connect initialises the Redis client using the REDIS_URL environment variable.
// Falls back to localhost:6379 when the variable is not set.
func Connect() error {
	addr := os.Getenv("REDIS_URL")
	if addr == "" {
		addr = "localhost:6379"
	}

	RDB = goredis.NewClient(&goredis.Options{
		Addr: addr,

		// Explicit pool sizing — default (10×GOMAXPROCS ≈ 40) exhausts under
		// 10k concurrent goroutines. 512 matches the buy-handler semaphore
		// (256 in-flight /buy + headroom for /pay, workers, reaper).
		PoolSize:     512,
		MinIdleConns: 16,

		// Tight timeouts so a slow Redis op fails fast rather than piling up.
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,

		// How long a goroutine waits for a free connection from the pool.
		// Shorter than ReadTimeout so the caller sees a pool error before a
		// read timeout, making the failure mode explicit.
		PoolTimeout: 3 * time.Second,
	})

	ctx := context.Background()
	if err := RDB.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis: ping failed: %w", err)
	}
	return nil
}
