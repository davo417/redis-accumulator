import clickhouse_connect
from pathlib import Path
from uuid import uuid4
from glob import glob
import redis


def on_starting(server):
    r = redis.Redis(host="localhost", decode_responses=False)
    c = clickhouse_connect.get_client(host='localhost', port=8123, username='default', database='redis')
    c.command("DROP TABLE IF EXISTS test")
    c.command("CREATE TABLE IF NOT EXISTS test (user_id UInt32, username String, active Bool, rate Float64) ENGINE=MergeTree() PRIMARY KEY (user_id)")
    
    def load_script(path):
        with open(path) as script:
            hash = r.register_script(script.read())
        return hash

    scripts = {Path(n).stem: load_script(n) for n in glob("../lua/*.lua")}
    
    r.flushall()
    scripts['schema'](keys=['test', 'user_id', 'username', 'active', 'rate'], args=['UInt32', 'String', 'Bool', 'Float64'])
    scripts['swap'](args=[str(uuid4())])
