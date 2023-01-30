#! /bin/bash

echo "START"

# flush data from db
redis-cli flushall > /dev/null
echo "FLUSH"
echo

# set test schema
redis-cli --eval ../src/schema.lua 'test' 'a' 'b' 'c', 'Int32' 'Float64' 'Bool' 'String' > /dev/null
echo "Schema:"
echo "$(redis-cli hgetall schema:test)"
echo

# set chunk id
redis-cli --eval ../src/swap.lua , 123 > /dev/null
# add records
redis-cli --eval ../src/insert.lua 'a' 'b' 'c' 'd' , 1  2.1    false   'hello' > /dev/null
redis-cli --eval ../src/insert.lua 'a' 'b' 'c' 'd' , 3  1.33   true    'world' > /dev/null
# check chunk length
echo "Lenght: $(redis-cli get chunk:123:len)"
echo

# swap chunk
redis-cli --eval ../src/swap.lua , 456
# export previous chunk with test schema
echo "$(redis-cli --eval ../src/extract.lua 'a' 'b' 'c' 'd' , 'test')" | python -m json.tool
# clean previous chunk data
redis-cli --eval ../src/clean.lua > /dev/null
echo

# add records
redis-cli --eval ../src/insert.lua 'a' 'b' 'c' 'd' , 5  0.5     true    'test'          > /dev/null
redis-cli --eval ../src/insert.lua 'a' 'b' 'c' 'd' , 10 4.12    false   'of'            > /dev/null
redis-cli --eval ../src/insert.lua 'a' 'b' 'c' 'd' , 8  8.333   true    'redis'         > /dev/null
redis-cli --eval ../src/insert.lua 'a' 'b' 'c' 'd' , 1  6.1     false   'accumulator'   > /dev/null
# check chunk length
echo "Lenght: $(redis-cli get chunk:456:len)"
echo

# swap chunk
redis-cli --eval ../src/swap.lua , 789 > /dev/null
# export previous chunk with test schema
echo "$(redis-cli --eval ../src/extract.lua 'all' , 'test')" | python -m json.tool
# clean previous chunk data
redis-cli --eval ../src/clean.lua > /dev/null
echo

echo "END"