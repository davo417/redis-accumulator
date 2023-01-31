-- init.lua
-- Initialize chunk ids if not set

-- ARGV: maximun chunk length

-- e.g.: redis-cli --eval init.lua


-- Exit if current chunk id is set
if redis.call("exists", "chunk:id:current") == 1 then
    return nil
end

-- Set maximun chunk length
if ARGV[1] ~= nil then
    redis.call("set", "chunk:max:len", ARGV[1])
end

-- Init current chunk id
redis.call("set", "chunk:id:current", 0)
-- Init current chunk length
return redis.call("set", "chunk:0:len", 0);
