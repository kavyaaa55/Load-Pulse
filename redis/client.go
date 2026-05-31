package redis

import (
	"context"
	"fmt"
	"os"

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
	})

	ctx := context.Background()
	if err := RDB.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis: ping failed: %w", err)
	}
	return nil
}
