-- Lua 5.1 http://www.lua.org/manual/5.1/manual.html
-- The scipt simply executes XINFO GROUPS command.

-- KEYS[1] should be a Redis stream key
-- returns Lua table
return redis.call('xinfo', 'groups', KEYS[1])