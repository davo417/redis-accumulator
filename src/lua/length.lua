-- length.lua
-- Check current chunk length

-- e.g.: redis-cli --eval length.lua


-- Current chunk id
local id = redis.call("get", "chunk:id:current");

-- Exit if no current chunk is set
if not id then
    return nil;
end

-- Return current chunk length
return redis.call("get", "chunk:"..id..":len");
