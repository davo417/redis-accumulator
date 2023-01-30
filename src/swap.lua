-- swap.lua
-- Swap last chunk for export and set new current chunk id

-- ARGV: new chunk id

-- e.g.: redis-cli --eval sawp.lua , 'new_id'


-- Current chunk id
local id = redis.call("get", "chunk:id:current");

-- Swap chunk id for export if current is set
if id then
    redis.call("set", "chunk:id:swap", id);
end

-- Set new chunk id
redis.call("set", "chunk:id:current", ARGV[1]);

-- Set new chunk lenght to 0
return redis.call("set", "chunk:"..ARGV[1]..":len", 0);