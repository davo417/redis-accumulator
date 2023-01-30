-- schema.lua
-- Set custom schema

-- KEYS: schema name, field names
-- ARGV: field types

-- e.g.: redis-cli --eval schema.lua 'field1' 'field2' , 'Int32' 'String'


-- Schema name
local schema = KEYS[1];

-- Fill schema hashmap
for index=2,table.getn(KEYS) do
    redis.call("hset", schema, KEYS[index], ARGV[index-1]);
end

return schema;