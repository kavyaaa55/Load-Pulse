package handlers

import (
	"encoding/json"
	"log"
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

// InventoryUpdate is the message pushed to all WebSocket clients whenever
// inventory or price changes.
type InventoryUpdate struct {
	ProductID string  `json:"product_id"`
	Count     int     `json:"count"`
	Price     float64 `json:"price"`
}

// UserEvent is pushed to all clients to update a single cell in the grid.
type UserEvent struct {
	Type      string `json:"type"`       // "user_event"
	UserIndex int    `json:"user_index"` // 0-9999 — which cell to update
	State     string `json:"state"`      // waiting | won | lost | ratelimit | expired
}

// SimEvent is pushed to signal simulation start/stop.
type SimEvent struct {
	Type string `json:"type"` // "sim_start" | "sim_done"
	VUs  int    `json:"vus"`
}

// Hub maintains the set of active WebSocket clients and broadcasts messages.
type Hub struct {
	mu      sync.RWMutex
	clients map[*wsClient]struct{}
	broadcast chan []byte
}

type wsClient struct {
	conn *websocket.Conn
	send chan []byte
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// NewHub creates an initialised Hub ready to run.
func NewHub() *Hub {
	return &Hub{
		clients:   make(map[*wsClient]struct{}),
		broadcast: make(chan []byte, 50000), // large enough for 10k user events
	}
}

// Run starts the hub's event loop. Call this in a goroutine.
func (h *Hub) Run() {
	for msg := range h.broadcast {
		h.mu.RLock()
		for c := range h.clients {
			select {
			case c.send <- msg:
			default:
				// Slow client — drop the message rather than blocking.
			}
		}
		h.mu.RUnlock()
	}
}

// BroadcastInventory serialises an InventoryUpdate and sends it to all clients.
func (h *Hub) BroadcastInventory(productID string, count int, price float64) {
	msg, err := json.Marshal(InventoryUpdate{
		ProductID: productID,
		Count:     count,
		Price:     price,
	})
	if err != nil {
		log.Printf("ws: marshal error: %v", err)
		return
	}
	h.broadcast <- msg
}

// BroadcastUserEvent sends a cell state update to all connected clients.
func (h *Hub) BroadcastUserEvent(userIndex int, state string) {
	msg, err := json.Marshal(UserEvent{
		Type:      "user_event",
		UserIndex: userIndex,
		State:     state,
	})
	if err != nil {
		return
	}
	h.broadcast <- msg // blocking — never drop user events
}

// BroadcastSimEvent sends a simulation lifecycle event to all clients.
func (h *Hub) BroadcastSimEvent(eventType string, vus int) {
	msg, err := json.Marshal(SimEvent{
		Type: eventType,
		VUs:  vus,
	})
	if err != nil {
		return
	}
	h.broadcast <- msg
}

// register adds a client to the hub.
func (h *Hub) register(c *wsClient) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
}

// unregister removes a client from the hub and closes its send channel.
func (h *Hub) unregister(c *wsClient) {
	h.mu.Lock()
	if _, ok := h.clients[c]; ok {
		delete(h.clients, c)
		close(c.send)
	}
	h.mu.Unlock()
}

// ServeWS upgrades an HTTP connection to WebSocket and registers the client.
func (h *Hub) ServeWS(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("ws: upgrade error: %v", err)
		return
	}

	client := &wsClient{
		conn: conn,
		send: make(chan []byte, 50000), // large enough to buffer all sim events
	}
	h.register(client)

	// Writer goroutine — pumps messages from the send channel to the wire.
	go func() {
		defer func() {
			h.unregister(client)
			conn.Close()
		}()
		for msg := range client.send {
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		}
	}()

	// Reader goroutine — keeps the connection alive and handles close frames.
	go func() {
		defer h.unregister(client)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()
}
