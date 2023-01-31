-- clean.lua
-- Delete old chunk data

-- KEYS: chunk ids to clean, "all" resolves as all swaped chunks

-- e.g.: redis-cli --eval clean.lua 'chunk1' 'chunk2'
-- e.g.: redis-cli --eval clean.lua 'all'


--resolve chunk ids to clean
local ids = {};
if KEYS[1] == "all" then
    ids = redis.call("smembers", "chunk:id:clean");
else
    local i = 0;
    for _, id in pairs(KEYS) do
        if redis.call("sismember", "chunk:id:clean", id) == 1 then
            ids[i] = id;
            i = i+1;
        end
    end
end

-- Exit if no chunks are available for cleaning
if table.getn(ids) == 0 then
    return 0
end

-- Total cleaned rows
local cleaned = 0;
local cleaned_l;

for _, id in pairs(ids) do
    -- Delete chunk fields
    local fields = redis.call("keys", "chunk:"..id..":field:*");
    if table.getn(fields) > 0 then
        redis.call("del", unpack(fields));
    end

    -- Accumulate cleaned rows
    cleaned_l = tonumber(redis.call("get", "chunk:"..id..":len"));
    if cleaned_l ~= nil then
        cleaned = cleaned + cleaned_l;
    end

    -- Delete chunk length
    redis.call("del", "chunk:"..id..":len");

    -- Remove chunk id
    redis.call("srem", "chunk:id:clean", id);
end

return cleaned;
