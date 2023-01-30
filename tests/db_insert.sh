#! /bin/bash

echo "START"

# CLICKHOUSE DB
clickhouse-client --query="DROP DATABASE IF EXISTS redis" > /dev/null
clickhouse-client --query="CREATE DATABASE IF NOT EXISTS redis" > /dev/null
clickhouse-client --query="""CREATE TABLE redis.test (user_id UInt32, username String, active Bool, rate Float64) ENGINE=MergeTree() PRIMARY KEY (user_id)""" > /dev/null

# REDIS DB
redis-cli flushall > /dev/null

# set matching schema
redis-cli --eval ../src/schema.lua 'test' 'user_id' 'username' 'active' 'rate' , 'UInt32' 'String' 'Bool' 'Float64' > /dev/null

# set chunk id
redis-cli --eval ../src/swap.lua , $(uuidgen -r) > /dev/null
# add records
redis-cli --eval ../src/insert.lua 'user_id' 'username' 'active' 'rate' , 1 'user1' false 5.0> /dev/null
redis-cli --eval ../src/insert.lua 'user_id' 'username' 'active' 'rate' , 2 'user2' true  4.5> /dev/null
# swap chunk
redis-cli --eval ../src/swap.lua , $(uuidgen -r) > /dev/null
# export previous chunk with test schema
redis-cli --eval ../src/export.lua 'user_id' 'username' 'active' 'rate' , 'test' | clickhouse-client --query="INSERT INTO redis.test FORMAT JSONColumnsWithMetadata"
# clean previous chunk data
redis-cli --eval ../src/clean.lua > /dev/null

# add records
redis-cli --eval ../src/insert.lua 'user_id' 'username' 'active' 'rate' , 3 'user3' true  3.1> /dev/null
redis-cli --eval ../src/insert.lua 'user_id' 'username' 'active' 'rate' , 4 'user4' false 1.6> /dev/null
redis-cli --eval ../src/insert.lua 'user_id' 'username' 'active' 'rate' , 5 'user5' true  0.3> /dev/null
redis-cli --eval ../src/insert.lua 'user_id' 'username' 'active' 'rate' , 6 'user6' false 2.9> /dev/null
# swap chunk
redis-cli --eval ../src/swap.lua , $(uuidgen -r) > /dev/null
# export previous chunk with test schema
redis-cli --eval ../src/export.lua 'all' , 'test' | clickhouse-client --query="INSERT INTO redis.test FORMAT JSONColumnsWithMetadata"
# clean previous chunk data
redis-cli --eval ../src/clean.lua > /dev/null

clickhouse-client --query="SELECT * FROM redis.test"

echo "END"