-- export.lua
-- Export a chunk in json format

-- KEYS: chunk id, schema name
-- ARGV: field names, "all" resolve as all fields in schema

-- e.g.: redis-cli --eval export.lua 'chunk1' 'schema' , 'field1' 'field2'
-- e.g.: redis-cli --eval export.lua 'chunk1' 'schema' , 'all'


-- Empty payload table
local payload = {};

local id = KEYS[1]
-- Return empty payload if swaped chunk is invalid
if redis.call("sismember", "chunk:id:swap", id) == 0 then
    return nil
end

-- Swaped chunk lenght
local len = tonumber(redis.call("get", "chunk:"..id..":len"));

-- Return empty payload if swaped chunk is empty
if (not len) or (len == 0) then
    return nil
end

-- Resolve field names for schema
local fields;
if ARGV[1] == "all" then
    fields = redis.call("hkeys", "schema:"..KEYS[2]);
else
    fields = ARGV;
end

-- Get field types for schema
local schema = redis.call("hmget", "schema:"..KEYS[2], unpack(fields));


-- String to Bool converter
local function tobool(val)
    if (val == nil) then
        return false;
    end
    return string.lower(val) == "true";
end

-- String prefix check
local function startswith(val, prefix)
    if type(val) ~= "string" then
        return false
    end
    return val:sub(1, #prefix) == prefix
end


-- Fill payload with meta-data and values
for fieldi, field in ipairs(fields) do
    -- Field type
    local dtype = schema[fieldi];

    -- Check field type and set appropiate converter
    local converter;
    if startswith(dtype, "Int") or startswith(dtype, "UInt") or startswith(dtype, "Float") then
        -- Interger type
        converter = tonumber
    elseif dtype == "Bool" then
        -- Boolean type
        converter = tobool;
    elseif (dtype == "String") or startswith(dtype, "Date") or startswith(dtype, "Time") then
        -- String | Date | Time | DateTime types, no conversion
        converter = false;
    else
        -- Invalid field type
        converter = nil;
    end

    if converter ~= nil then
        -- Get field values
        local data = redis.call("lrange", "chunk:"..id..":field:"..field, 0, len);

        -- Convert string values to number or booleanif needed
        if converter then
            for index, value in ipairs(data) do
                data[index] = converter(value);
            end
        end

        -- Set field values
        payload[field] = data;
    end
end

-- Return json encoded payload
return cjson.encode(payload);