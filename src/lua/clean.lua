-- clean.lua
-- Delete old chunk data

-- e.g.: redis-cli --eval clean.lua


-- Swaped chunk id
local id = redis.call("get", "chunk:id:swap");

-- Exis if no swaped chunk is set
if not id then
    return nil;
end

-- Delete swaped chunk lenght
redis.call("del", "chunk:"..id..":len");

-- Delete swaped chunk fields
local fields = redis.call("keys", "chunk:"..id..":field:*");
if table.getn(fields) > 0 then
    redis.call("del", unpack(fields));
end

-- Delete swaped chunk id
return redis.call("del", "chunk:id:swap");
