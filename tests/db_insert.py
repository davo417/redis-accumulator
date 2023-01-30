
import clickhouse_connect
from pathlib import Path
from uuid import uuid4
from glob import glob
import redis
import json


c = clickhouse_connect.get_client(host='localhost', port=8123, username='default', database='redis')
c.command("DROP TABLE IF EXISTS test")
c.command("CREATE TABLE IF NOT EXISTS test (user_id UInt32, username String, active Bool, rate Float64) ENGINE=MergeTree() PRIMARY KEY (user_id)")

r = redis.Redis("localhost", decode_responses=False)
r.flushall()


def load_script(path):
    with open(path) as script:
        hash = r.script_load(script.read())
    return hash

scripts = {Path(n).stem: load_script(n) for n in glob("src/lua/*.lua")}

def run(name, nkeys=0, *args):
    return r.evalsha(scripts[name], nkeys, *args)

schema = run('schema', 5, 'test', 'user_id', 'username', 'active', 'rate', 'UInt32', 'String', 'Bool', 'Float64')
# print("Schema: ", r.hgetall(f"schema:{schema}"))

run('swap', 0, str(uuid4()))
chunk_id = r.get("chunk:id:current")
# print(f"Chunk id: {chunk_id}")

run('insert', 4, 'user_id', 'username', 'active', 'rate', 1, 'user1', 'false', 5.0)
run('insert', 4, 'user_id', 'username', 'active', 'rate', 2, 'user2', 'true', 4.5)
# print("Chunk len: ", r.get(f"chunk:{chunk_id}:len"))

run('swap', 0, str(uuid4()))

data = json.loads(run('export', 4, 'user_id', 'username', 'active', 'rate', 'test'))
c.insert('test', data=list(data['data'].values()), column_names=list(data['data'].keys()), column_oriented=True)

run('clean')

run('insert', 4, 'user_id', 'username', 'active', 'rate', 3, 'user3', 'false', 3.1)
run('insert', 4, 'user_id', 'username', 'active', 'rate', 4, 'user4', 'true', 1.6)
run('insert', 4, 'user_id', 'username', 'active', 'rate', 5, 'user5', 'false', 0.3)
run('insert', 4, 'user_id', 'username', 'active', 'rate', 6, 'user6', 'true', 2.9)

run('swap', 0, str(uuid4()))

data = json.loads(run('export', 1, 'all', 'test'))
c.insert('test', data=list(data['data'].values()), column_names=list(data['data'].keys()), column_oriented=True)

run('clean')

print(c.query("SELECT * FROM test").result_rows)