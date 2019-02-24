-- Lua 5.1 http://www.lua.org/manual/5.1/manual.html
-- The scipt allows to execute batch of XADD insertions in a Redis stream.
-- XADD command in Redis accepts only one message insertion, so an insertion 
-- of hundreads of thousands messages in a loop is relatively slow.
-- The script moves the loop to Redis site, so the execution becomes much faster.

-- KEYS[1] should be a Redis stream key
-- ARGV list should contain message fields in JSON Object format (ex. "{\"q\": \"1\", \"w\": \"2\"}")
-- returns list of ids have been inserted into the stream

-- Transforms a given table in following way:
-- When input table is {'q': '1', 'w': '2'} then output table is {'q', '1', 'w', '2'}.
-- So both keys and values in input table becomes values in output
-- and all of them are indexed by numbers.
local function flattenTable(t)
	local out = {}
	for k, v in pairs(t) do
		table.insert(out, k)
		table.insert(out, v)
	end
	return out
end

local ids = {}
for _, jsonText in ipairs(ARGV) do
	local json = cjson.decode(jsonText)
	local messageFields = flattenTable(json)
	local id = redis.call('XADD', KEYS[1], '*', unpack(messageFields))
	table.insert(ids, id)
end
return ids