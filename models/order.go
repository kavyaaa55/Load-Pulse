package models

import "time"

// Order represents a completed purchase record stored in Postgres.
type Order struct {
	ID        int64     `json:"id"`
	UserID    string    `json:"user_id"`
	ProductID string    `json:"product_id"`
	Price     float64   `json:"price"`
	Timestamp time.Time `json:"timestamp"`
	Status    string    `json:"status"`
}
