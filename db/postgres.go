package db

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	_ "github.com/lib/pq" // Postgres driver

	"github.com/livesale/live-sale/models"
)

// DB wraps the standard sql.DB to provide application-level helpers.
type DB struct {
	conn *sql.DB
}

// Connect opens a connection to Postgres using the POSTGRES_URL environment
// variable and creates the orders table if it does not already exist.
func Connect() (*DB, error) {
	dsn := os.Getenv("POSTGRES_URL")
	if dsn == "" {
		dsn = "postgres://user:pass@localhost:5432/livesale?sslmode=disable"
	}

	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("db: open failed: %w", err)
	}

	if err := conn.PingContext(context.Background()); err != nil {
		return nil, fmt.Errorf("db: ping failed: %w", err)
	}

	d := &DB{conn: conn}
	if err := d.migrate(); err != nil {
		return nil, err
	}
	return d, nil
}

// migrate creates the orders table when it does not exist.
func (d *DB) migrate() error {
	const ddl = `
CREATE TABLE IF NOT EXISTS orders (
    id         BIGSERIAL PRIMARY KEY,
    user_id    TEXT        NOT NULL,
    product_id TEXT        NOT NULL,
    price      NUMERIC     NOT NULL,
    timestamp  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status     TEXT        NOT NULL
);`
	_, err := d.conn.ExecContext(context.Background(), ddl)
	if err != nil {
		return fmt.Errorf("db: migrate failed: %w", err)
	}
	return nil
}

// InsertOrder persists a completed order to Postgres.
func (d *DB) InsertOrder(order models.Order) error {
	const query = `
INSERT INTO orders (user_id, product_id, price, timestamp, status)
VALUES ($1, $2, $3, $4, $5)`

	_, err := d.conn.ExecContext(
		context.Background(),
		query,
		order.UserID,
		order.ProductID,
		order.Price,
		order.Timestamp,
		order.Status,
	)
	if err != nil {
		return fmt.Errorf("db: insert order failed: %w", err)
	}
	return nil
}
