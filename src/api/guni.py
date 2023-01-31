def on_starting(server):
    import clickhouse_connect
    from pathlib import Path
    from glob import glob
    import redis
    import os

    c = clickhouse_connect.get_client(host='localhost', port=8123, username='default', database='redis')
    c.command("DROP TABLE IF EXISTS test")
    c.command("""
        CREATE TABLE IF NOT EXISTS test (
                user_id UInt32,
                username String,
                active Bool,
                rate Float64
        ) ENGINE=MergeTree()
        PRIMARY KEY (user_id)
    """)
    
    r = redis.Redis(host="localhost", decode_responses=False)
    r.flushall()

    def load_script(path):
        with open(path) as script:
            hash = r.register_script(script.read())
        return hash

    scripts = {Path(n).stem: load_script(n) for n in glob("../lua/*.lua")}
    scripts['schema'](keys=['test', 'user_id', 'username', 'active', 'rate'], args=['UInt32', 'String', 'Bool', 'Float64'])
    max_length = os.environ.get("REDIS_CHUNK_MAX_LEN", 10000)
    scripts['init'](args=[max_length])


access_log_format = '%(h)s %(l)s %(u)s %(t)s "%(r)s" %(s)s %(b)s "%(f)s" "%(a)s"'
worker_class = 'uvicorn.workers.UvicornWorker'
wsgi_app = 'api.api:app'
accesslog = 'access.log'
bind = '127.0.0.1:8000'
capture_output = True
pythonpath = '..'
errorlog = '-'
reload = True
workers = 4