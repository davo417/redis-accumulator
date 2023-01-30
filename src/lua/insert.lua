-- insert.lua
-- Accumulate a record in the current chunk

-- KEYS: field names
-- ARGV: values

-- e.g.: redis-cli --eval insert.lua 'field1' 'field2' , 'value1' 'value2'


-- Get current chunk id for accumulation 
local id = redis.call("get", "chunk:id:current");

-- Exit if the current chunk id is not set, must use swap.lua to set a new one
if not id then
    return nil;
end

-- Append each value to their field list
for index, key in ipairs(KEYS) do
    redis.call("rpush", "chunk:"..id..":field:"..key, ARGV[index]);
end

-- Increment chunk lenght counter
return redis.call("incr", "chunk:"..id..":len");