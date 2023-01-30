from pydantic import parse_obj_as
from pydantic import BaseModel
import redis.asyncio as redis
from fastapi import FastAPI
from fastapi import status
import clickhouse_connect
from pathlib import Path
from uuid import uuid4
import orjson as json
from glob import glob


app = FastAPI()
r = redis.Redis(host="localhost", decode_responses=False)
c = clickhouse_connect.get_client(host='localhost', port=8123, username='default', database='redis')

def load_script(path):
    with open(path) as script:
        hash = r.register_script(script.read())
    return hash

scripts = {Path(n).stem: load_script(n) for n in glob("../lua/*.lua")}


class Test(BaseModel):
    user_id: int
    username: str
    active: bool
    rate: float


@app.post("/create/", status_code=status.HTTP_201_CREATED)
async def create(test: Test) -> int:
    test: dict = test.dict()
    test['active'] = str(test['active']).lower()
    return await scripts['insert'](keys=list(test.keys()), args=list(test.values()))

@app.get("/flush/")
async def flush() -> list[Test]:
    await scripts['swap'](args=[str(uuid4())])
    data = json.loads(await scripts['export'](keys=['user_id', 'username', 'active', 'rate'], args=['test']))
    data = data.get('data', dict())
    if len(data) == 0:
        res = []
    else:
        c.insert('test', data=list(data.values()), column_names=list(data.keys()), column_oriented=True)
        res = c.query("SELECT * FROM test LIMIT 10").named_results()
    await scripts['clean']()
    return parse_obj_as(list[Test], res)