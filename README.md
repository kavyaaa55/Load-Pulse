<div align="center">

# ⚡ Load-Pulse

**Real-time flash sale backend · 10,000 concurrent users · zero oversell**

![Go](https://img.shields.io/badge/Go-1.21-00ADD8?style=flat-square&logo=go&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-7-DC382D?style=flat-square&logo=redis&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?style=flat-square&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker&logoColor=white)
![k6](https://img.shields.io/badge/k6-Load%20Testing-7D64FF?style=flat-square&logo=k6&logoColor=white)

</div>

---

## What Is This

A backend system that handles flash sales under extreme concurrency. When 10,000 users simultaneously hit "Buy" for 100 items, exactly 100 people get it. No overselling, no underselling.

Think BookMyShow or Meesho Live — not a regular ecommerce site. The hard problem here is **race conditions**: two users reading inventory at the same millisecond, both thinking they got the last item. This project solves that with Redis Lua scripts (atomic check + decrement), a virtual queue, and slot-based reservations with TTL.

```
Resume line:
Built a real-time flash sale platform handling 10k concurrent users across multiple
products with zero oversells — atomic inventory via Redis Lua scripts, virtual queue
with TTL-based slot expiry, dynamic pricing, and ~7,000 req/s throughput via k6.
```

---

## Architecture

![Architecture Diagram](architecture.png)

### Key design decisions

**Redis is the single source of truth.** PostgreSQL is never read during the sale — only written to asynchronously after payment succeeds.

**Inventory decrements at slot-assignment time, not at queue-join time.** The queue is just a waiting room. An item is only "locked" for a user when the queue worker pops them and runs the Lua script.

**Gate check at `/buy` prevents queue flooding.** Before joining the queue: `LLEN queue + SCARD active_slots >= inventory → reject 409`. This ensures only users who can realistically get an item enter the queue.

---

## Tech Stack

| Component | Tool | Why |
|---|---|---|
| Language | Go 1.21 | Goroutines handle 10k concurrent requests cheaply |
| HTTP Framework | Gin | Minimal, fast REST API |
| State store | Redis 7 | Atomic Lua scripts, TTL, Lists — live sale state |
| Database | PostgreSQL 15 | Permanent order records only |
| WebSocket | gorilla/websocket | Push live inventory to all connected clients |
| Load testing | k6 | 10k virtual users, p95/p99 latency metrics |
| Containerization | Docker Compose | One command to run everything |

---

## Features

- **Multiple products simultaneously** — 5 products, each with independent inventory, queue, and pricing
- **Atomic inventory** — Redis Lua script: check + decrement in one operation, zero race conditions
- **Virtual queue** — FIFO, position tracking, per-user rate limiting
- **Slot reservation with TTL** — 120s payment window, auto-releases on expiry
- **Per-user purchase limit** — `bought:userID` flag prevents double-buying
- **Mock payment** — 80% success / 20% fail; failures return inventory and allow reassignment
- **Real-time WebSocket broadcast** — live inventory count pushed to all clients on every change
- **Dynamic pricing** — price surges 20% when stock drops to ≤ 10 units
- **Anti-cheat** — per-IP rate limiting (production mode)

---

## Project Structure

```
load-pulse/
├── main.go                  — server entry, product seeding, worker startup
├── docker-compose.yml
├── Dockerfile
├── handlers/
│   ├── buy.go               — POST /buy · gate check · enqueue
│   ├── inventory.go         — GET /inventory/:productID
│   └── websocket.go         — WebSocket hub + broadcaster
├── queue/
│   └── worker.go            — goroutine per product (LPOP → Lua → slot assign)
├── redis/
│   ├── client.go            — Redis connection
│   └── scripts.go           — Lua script loader
├── payment/
│   └── mock.go              — 80/20 payment simulation
├── models/
│   └── order.go             — Order struct
├── db/
│   └── postgres.go          — connection + InsertOrder
└── load-test/
    └── script.js            — k6 script · 10k VUs · 10s
```

---

## Getting Started

**Prerequisites:** Docker, Docker Compose

```bash
# Clone and start
git clone https://github.com/your-username/Load-Pulse
cd Load-Pulse
docker compose up --build -d

# Verify
curl http://localhost:8080/inventory/iphone15
# → {"product_id":"iphone15","count":100,"price":999}

# Logs
docker compose logs app -f
```

Products are seeded automatically on startup — no manual setup needed.

---

## API Reference

```
POST /buy
  Body:  { "user_id": "uid_123", "product_id": "iphone15" }
  200 →  { "status": "queued", "position": 2847 }
  409 →  { "error": "sold out" }
  429 →  { "error": "rate limited" }

POST /pay
  Body:  { "user_id": "uid_123", "product_id": "iphone15" }
  200 →  { "status": "ORDER_CONFIRMED" }
  402 →  { "status": "PAYMENT_FAILED" }
  410 →  { "status": "SLOT_EXPIRED" }

GET /inventory/:productID
  200 →  { "product_id": "iphone15", "count": 47, "price": 999 }

GET /queue/position/:userID/:productID
  200 →  { "position": 142 }

WS /ws
  Push → { "product_id": "iphone15", "count": 46, "price": 999 }
```

---

## Load Testing

```bash
# Install k6 — https://k6.io/docs/get-started/installation/

# Reset state before every run
docker compose exec redis redis-cli FLUSHDB
docker compose restart app

# Run
k6 run load-test/script.js
```

### Results (10,000 VUs · 10s)

![Load Test Results](screenshot.png)

```
Total requests      : ~80,000
Throughput          : ~7,300 req/s
Confirmed orders    : 86   (80% of 100 + payment success rate ≈ correct)
Payment failed      : 15   (20% mock failure rate ≈ correct)
Zero oversell       : ✓
80/20 payment ratio : ✓
```

---

## Redis Keys

```
inv:iphone15         →  "97"              inventory counter
queue:iphone15       →  [uid1, uid2]      FIFO queue (Redis List)
slot:uid_123         →  "1"  TTL 120s     payment reservation
slots:iphone15       →  {uid1, uid2}      Set — users in payment window
bought:uid_123       →  "1"              permanent — no double buy
ratelimit:uid_123    →  "1"  TTL 60s     per-user rate limit
price:iphone15       →  "999"            current price, surges at ≤10 units
```

---

## The Lua Script

The heart of the system. Runs inside Redis atomically — no race condition possible.

```lua
local inv_key    = KEYS[1]   -- inv:productID
local slot_key   = KEYS[2]   -- slot:userID
local bought_key = KEYS[3]   -- bought:userID
local ttl        = ARGV[1]   -- 120

if redis.call('EXISTS', bought_key) == 1 then return "ALREADY_BOUGHT" end

local count = tonumber(redis.call('GET', inv_key))
if count == nil or count <= 0 then return "SOLD_OUT" end

redis.call('DECR', inv_key)
redis.call('SET', slot_key, 1)
redis.call('EXPIRE', slot_key, ttl)
return "OK"
```

---


## Production Extensions

- Nginx in front of multiple Go instances for horizontal scaling
- Redis Cluster for high availability
- Prometheus + Grafana for real-time metrics dashboard
- Re-enable IP-based rate limiting (currently disabled for load testing)
- Two-phase slot state (`reserved` → `paying`) to handle TTL race during bank API calls