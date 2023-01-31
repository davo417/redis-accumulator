#! /bin/bash

echo "START"

# flush data from db
redis-cli flushall > /dev/null
echo "FLUSH"
echo

LUA_HOME='./src/lua'

# init db
redis-cli --eval $LUA_HOME/init.lua , 5 > /dev/null

# set test schema
redis-cli --eval $LUA_HOME/schema.lua 'test' 'a' 'b' 'c' 'd' , 'Int32' 'Float64' 'Bool' 'String' > /dev/null
echo "Schema:"
echo "$(redis-cli hgetall schema:test)"
echo

# add records
redis-cli --eval $LUA_HOME/insert.lua 'a' 'b' 'c' 'd' , 1  2.1    false   'hello' > /dev/null
redis-cli --eval $LUA_HOME/insert.lua 'a' 'b' 'c' 'd' , 3  1.33   true    'world' > /dev/null
# check chunk length
echo "Lenght: $(redis-cli get chunk:0:len)"
echo

# swap chunk
redis-cli --eval $LUA_HOME/swap.lua > /dev/null
# export previous chunk with test schema
echo "$(redis-cli --eval $LUA_HOME/export.lua '0' 'test' , 'a' 'b' 'c' 'd')" | python -m json.tool
# clean previous chunk data
redis-cli --eval $LUA_HOME/clean.lua > /dev/null
echo

# add records
redis-cli --eval $LUA_HOME/insert.lua 'a' 'b' 'c' 'd' , 5  0.5     true    'test'          > /dev/null
redis-cli --eval $LUA_HOME/insert.lua 'a' 'b' 'c' 'd' , 10 4.12    false   'of'            > /dev/null
redis-cli --eval $LUA_HOME/insert.lua 'a' 'b' 'c' 'd' , 8  8.333   true    'redis'         > /dev/null
redis-cli --eval $LUA_HOME/insert.lua 'a' 'b' 'c' 'd' , 1  6.1     false   'accumulator'   > /dev/null
redis-cli --eval $LUA_HOME/insert.lua 'a' 'b' 'c' 'd' , 5  0.5     true    'test'          > /dev/null
# new chunk created
redis-cli --eval $LUA_HOME/insert.lua 'a' 'b' 'c' 'd' , 10 4.12    false   'of'            > /dev/null
redis-cli --eval $LUA_HOME/insert.lua 'a' 'b' 'c' 'd' , 8  8.333   true    'redis'         > /dev/null
redis-cli --eval $LUA_HOME/insert.lua 'a' 'b' 'c' 'd' , 1  6.1     false   'accumulator'   > /dev/null
# check chunk length
echo "Lenght: $(redis-cli get chunk:1:len)"
echo "Lenght: $(redis-cli get chunk:2:len)"
echo

# swap chunk
redis-cli --eval $LUA_HOME/swap.lua > /dev/null
# export previous chunk with test schema
echo "$(redis-cli --eval $LUA_HOME/export.lua '1' 'test' , 'all')" | python -m json.tool
echo "$(redis-cli --eval $LUA_HOME/export.lua '2' 'test' , 'all')" | python -m json.tool
# clean previous chunk data
redis-cli --eval $LUA_HOME/clean.lua > /dev/null
echo

echo "END"