-- lenght.lua
-- Check current or swaped chunk length

-- KEYS: chunk name: current/swaped

-- e.g.: redis-cli --eval lenght.lua 'name'


-- Exit if chunk name is not valid
local name = KEYS[1]
if (name ~= "current") and (name ~= "swaped") then
    return nil
end

-- Current chunk id
local id = redis.call("get", "chunk:id:"..name);

-- Exit if no chunk is set
if not id then
    return nil;
end

-- Return chunk lenght
return redis.call("get", "chunk:"..id..":len");
