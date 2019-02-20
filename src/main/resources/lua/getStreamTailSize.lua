-- Lua 5.1 http://www.lua.org/manual/5.1/manual.html
-- The scipt calculates the number of messages in the Redis stream
-- which were never delivered to the reading group.
-- The execution in the Lua script is more effective because there is no need to
-- tramsfer whole xrange result to the client just to calculate the lenth.

-- KEYS[1] should be a Redis stream key
-- ARGV[1] should be an incremented last-delivered-id for the reading group.
local xrange = redis.call('xrange', KEYS[1], ARGV[1], '+')
return #xrange