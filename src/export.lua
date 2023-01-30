-- export.lua
-- Export a chunk in json format

-- KEYS: field names
-- ARGV: schema id, "all" resolve as all fields in schema

-- e.g.: redis-cli --eval export.lua 'field1' 'field2' , 'schema'
-- e.g.: redis-cli --eval export.lua 'all' , 'schema'


-- Empty payload table
local payload = {meta={}, data={}, rows=0};

-- Swaped chunk id
local id = redis.call("get", "chunk:id:swap");

-- Return empty payload if swaped chunk is not set
if not id then
    return cjson.encode(payload);
end

-- Swaped chunk lenght
local len = tonumber(redis.call("get", "chunk:"..id..":len"));

-- Return empty payload if swaped chunk is empty
if not len then
    return cjson.encode(payload);
end

-- Set payload row count
payload.rows = len;

-- Resolve field names for schema
local keys;
if KEYS[1] == "all" then
    keys = redis.call("hkeys", "schema:"..ARGV[1]);
else
    keys = KEYS;
end

-- Get field types for schema
local schema = redis.call("hmget", "schema:"..ARGV[1], unpack(keys));


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
for keyi, key in ipairs(keys) do
    -- Field type
    local dtype = schema[keyi];

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
        local data = redis.call("lrange", "chunk:"..id..":field:"..key, 0, len);

        -- Convert string values to number or booleanif needed
        if converter then
            for index, value in ipairs(data) do
                data[index] = converter(value);
            end
        end

        -- Set field meta-data, name and type
        payload.meta[keyi] = {name=key, type=dtype}
        -- Set field values
        payload.data[key] = data;
    end
end

-- Return json encoded payload
return cjson.encode(payload);