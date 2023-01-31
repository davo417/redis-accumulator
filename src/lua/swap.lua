-- swap.lua
-- Swap last chunk for export and set new current chunk id

-- e.g.: redis-cli --eval sawp.lua


-- Current chunk id
local id = redis.call("get", "chunk:id:current");

-- Swaped chunk length
local length = 0;

-- Swap chunk id for export if current is set
if id then
    redis.call("sadd", "chunk:id:swap", id);

    -- Get swaped chunk length
    length = redis.call("get", "chunk:"..id..":len");

    -- Increment current chunk id
    id = redis.call("incr", "chunk:id:current");
    
    -- Set new chunk length to 0
    redis.call("set", "chunk:"..id..":len", 0);
else
    -- Set new chunk id
    redis.call("set", "chunk:id:current", 0);

    -- Set new chunk length to 0
    redis.call("set", "chunk:0:len", 0);
end

return length;