package redis

import goredis "github.com/redis/go-redis/v9"

// atomicBuyLua is the Lua script that atomically:
//  1. Checks whether the user has already bought the product.
//  2. Checks whether inventory is available.
//  3. Decrements inventory and sets a reservation slot with a TTL.
//
// KEYS[1] = inv:<productID>
// KEYS[2] = slot:<userID>
// KEYS[3] = bought:<userID>
// ARGV[1]  = slot TTL in seconds
const atomicBuyLua = `
local inv_key    = KEYS[1]
local slot_key   = KEYS[2]
local bought_key = KEYS[3]
local ttl        = ARGV[1]

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

return "OK"
`

// AtomicBuyScript is the pre-loaded Redis script for atomic buy operations.
var AtomicBuyScript *goredis.Script

// LoadScripts registers all Lua scripts so they are ready to use.
func LoadScripts() {
	AtomicBuyScript = goredis.NewScript(atomicBuyLua)
}
