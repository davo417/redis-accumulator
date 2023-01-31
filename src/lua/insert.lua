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

-- Increment chunk length counter
local length = redis.call("incr", "chunk:"..id..":len");

-- Get maximun lenght for chunks
local max_length = redis.call("get", "chunk:max:len");

-- Swap chunk on fill
if  (max_length ~= nil) and (length >= tonumber(max_length)) then
    -- Swap current chunk id
    redis.call("sadd", "chunk:id:swap", id);

    -- Increment current chunk id
    id = redis.call("incr", "chunk:id:current");
    
    -- Set new chunk length to 0
    redis.call("set", "chunk:"..id..":len", 0);
end

return length