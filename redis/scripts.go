package redis

import goredis "github.com/redis/go-redis/v9"

// atomicBuyLua is the single script that handles the entire /buy hot path
// atomically. It replaces the previous 4-round-trip sequence
// (GET inv → LLEN queue → SCARD slots → RPUSH) with one Redis call,
// eliminating both the TOCTOU race and pool-exhaustion from sequential calls.
//
// The worker queue is gone from the /buy path — this script assigns the slot
// directly, so there is no queue-worker indirection for the buyer.
// The worker's atomicBuyLua (called from queue/worker.go for queued users)
// is the same script reused with a queue key.
//
// KEYS[1] = inv:<productID>        — inventory counter
// KEYS[2] = slot:<userID>          — reservation key  (SET with TTL)
// KEYS[3] = bought:<userID>        — permanent purchase flag
// KEYS[4] = slots:<productID>      — active-slots SET  (gate-check denominator)
// KEYS[5] = expiry:<productID>     — expiry sorted set (reaper input)
// ARGV[1] = slot TTL in seconds    — e.g. "120"
// ARGV[2] = expiry score           — Unix timestamp: now + TTL
// ARGV[3] = userID                 — member value for ZADD / SADD
//
// Returns:
//   "OK"            — slot assigned, inventory decremented
//   "SOLD_OUT"      — inv <= 0
//   "ALREADY_BOUGHT"— bought key exists
const atomicBuyLua = `
local inv_key    = KEYS[1]
local slot_key   = KEYS[2]
local bought_key = KEYS[3]
local slots_key  = KEYS[4]
local expiry_key = KEYS[5]
local ttl        = ARGV[1]
local exp_score  = ARGV[2]
local user_id    = ARGV[3]

if redis.call('EXISTS', bought_key) == 1 then
    return "ALREADY_BOUGHT"
end

local count = tonumber(redis.call('GET', inv_key))
if count == nil or count <= 0 then
    return "SOLD_OUT"
end

redis.call('DECR', inv_key)
redis.call('SET', slot_key, 1)
redis.call('EXPIRE', slot_key, ttl)
redis.call('ZADD', expiry_key, exp_score, user_id)
redis.call('SADD', slots_key, user_id)

return "OK"
`

// reaperLua atomically scans the expiry sorted set for slots whose deadline
// has passed (score <= now), and for each one:
//  1. Checks the slot key still exists — if /pay already cleaned it up, skip
//     to avoid double-returning inventory.
//  2. Returns the inventory unit (INCR).
//  3. Removes the user from the expiry sorted set (ZREM).
//  4. Removes the user from the active-slots SET used by the gate check (SREM).
//
// Returns the number of slots that were reaped.
//
// KEYS[1] = expiry:<productID>   — sorted set, score = unix expiry timestamp
// KEYS[2] = inv:<productID>      — inventory counter
// KEYS[3] = slots:<productID>    — active-slots SET (gate check)
// ARGV[1] = now                  — current unix timestamp (seconds)
// ARGV[2] = batch                — max slots to reap in one call
const reaperLua = `
local expiry_key = KEYS[1]
local inv_key    = KEYS[2]
local slots_key  = KEYS[3]
local now        = ARGV[1]
local batch      = tonumber(ARGV[2])

local expired = redis.call('ZRANGEBYSCORE', expiry_key, 0, now, 'LIMIT', 0, batch)
local reaped = 0

for _, user_id in ipairs(expired) do
    local slot_key = 'slot:' .. user_id
    -- Only return inventory if the slot key still exists.
    -- If /pay already ran, it deleted slot: and SRem'd from slots: — skip.
    if redis.call('EXISTS', slot_key) == 1 then
        redis.call('DEL', slot_key)
        redis.call('INCR', inv_key)
        reaped = reaped + 1
    end
    redis.call('ZREM', expiry_key, user_id)
    redis.call('SREM', slots_key, user_id)
end

return reaped
`

// payFailLua atomically handles a failed payment:
//  1. Verifies the slot key still exists (guards against double-return if the
//     reaper already ran between the /pay Exists check and now).
//  2. Returns the inventory unit (INCR).
//  3. Deletes the slot key.
//  4. Removes the user from the expiry sorted set.
//  5. Removes the user from the active-slots SET.
//
// Returns 1 if inventory was returned, 0 if the slot was already gone.
//
// KEYS[1] = slot:<userID>        — reservation key
// KEYS[2] = inv:<productID>      — inventory counter
// KEYS[3] = expiry:<productID>   — expiry sorted set
// KEYS[4] = slots:<productID>    — active-slots SET
// ARGV[1] = userID               — needed for ZREM / SREM
const payFailLua = `
local slot_key   = KEYS[1]
local inv_key    = KEYS[2]
local expiry_key = KEYS[3]
local slots_key  = KEYS[4]
local user_id    = ARGV[1]

if redis.call('EXISTS', slot_key) == 0 then
    return 0
end

redis.call('DEL', slot_key)
redis.call('INCR', inv_key)
redis.call('ZREM', expiry_key, user_id)
redis.call('SREM', slots_key, user_id)

return 1
`

// AtomicBuyScript is the pre-loaded Redis script for atomic buy operations.
var AtomicBuyScript *goredis.Script

// ReaperScript is the pre-loaded Redis script that returns inventory for
// expired slots. Called by the reaper goroutine every second.
var ReaperScript *goredis.Script

// PayFailScript is the pre-loaded Redis script for atomic payment-fail returns.
var PayFailScript *goredis.Script

// LoadScripts registers all Lua scripts so they are ready to use.
func LoadScripts() {
	AtomicBuyScript = goredis.NewScript(atomicBuyLua)
	ReaperScript    = goredis.NewScript(reaperLua)
	PayFailScript   = goredis.NewScript(payFailLua)
}
