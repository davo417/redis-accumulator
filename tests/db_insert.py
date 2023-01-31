import clickhouse_connect
from pathlib import Path
from uuid import uuid4
from glob import glob
import redis
import json


c = clickhouse_connect.get_client(host='localhost', port=8123, username='default', database='redis')
c.command("DROP TABLE IF EXISTS test")
c.command("CREATE TABLE IF NOT EXISTS test (user_id UInt32, username String, active Bool, rate Float64) ENGINE=MergeTree() PRIMARY KEY (user_id)")

r = redis.Redis("localhost", decode_responses=True)
r.flushall()


def load_script(path):
    with open(path) as script:
        hash = r.register_script(script.read())
    return hash

scripts = {Path(n).stem: load_script(n) for n in glob("src/lua/*.lua")}

# def run(name, nkeys=0, *args):
#     return r.evalsha(scripts[name], nkeys, *args)

scripts['init'](args=[5])
schema = scripts['schema'](keys=['test', 'user_id', 'username', 'active', 'rate'], args=['UInt32', 'String', 'Bool', 'Float64'])
# print("Schema: ", r.hgetall(f"schema:{schema}"))

chunk_id = r.get("chunk:id:current")
# print(f"Chunk id: {chunk_id}")

scripts['insert'](keys=['user_id', 'username', 'active', 'rate'], args=[1, 'user1', 'false', 5.0])
scripts['insert'](keys=['user_id', 'username', 'active', 'rate'], args=[2, 'user2', 'true', 4.5])
# print("Chunk len: ", r.get(f"chunk:{chunk_id}:len"))

scripts['swap']()

data = json.loads(scripts['export'](keys=[chunk_id, 'test'], args=['user_id', 'username', 'active', 'rate']))
c.insert('test', data=list(data['data'].values()), column_names=list(data['data'].keys()), column_oriented=True)

scripts['clean']()

chunk_id = r.get("chunk:id:current")
scripts['insert'](keys=['user_id', 'username', 'active', 'rate'], args=[3, 'user3', 'false', 3.1])
scripts['insert'](keys=['user_id', 'username', 'active', 'rate'], args=[4, 'user4', 'true', 1.6])
scripts['insert'](keys=['user_id', 'username', 'active', 'rate'], args=[5, 'user5', 'false', 0.3])
scripts['insert'](keys=['user_id', 'username', 'active', 'rate'], args=[6, 'user6', 'true', 2.9])
scripts['insert'](keys=['user_id', 'username', 'active', 'rate'], args=[3, 'user3', 'false', 3.1])
# new chunk created
n_chunk_id = r.get("chunk:id:current")
scripts['insert'](keys=['user_id', 'username', 'active', 'rate'], args=[4, 'user4', 'true', 1.6])
scripts['insert'](keys=['user_id', 'username', 'active', 'rate'], args=[5, 'user5', 'false', 0.3])
scripts['insert'](keys=['user_id', 'username', 'active', 'rate'], args=[6, 'user6', 'true', 2.9])

scripts['swap']()

data = json.loads(scripts['export'](keys=[chunk_id, 'test'], args=['all']))
c.insert('test', data=list(data['data'].values()), column_names=list(data['data'].keys()), column_oriented=True)
data = json.loads(scripts['export'](keys=[n_chunk_id, 'test'], args=['all']))
c.insert('test', data=list(data['data'].values()), column_names=list(data['data'].keys()), column_oriented=True)

scripts['clean']()
print(c.query("SELECT * FROM test").result_rows)