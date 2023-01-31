from fastapi import FastAPI, HTTPException, Depends
from aiochclient.exceptions import ChClientError
from fastapi.responses import ORJSONResponse
from aiohttp import ClientSession
from aiochclient import ChClient
from pydantic import BaseModel
import redis.asyncio as redis
from fastapi import status
from pathlib import Path
from glob import glob
import orjson


r = redis.Redis.from_url(
    url="unix:///var/run/redis/redis.sock",
    decode_responses=True,
    db=1
)

def load_script(path):
    with open(path) as script:
        hash = r.register_script(script.read())
    return hash

scripts = {Path(n).stem: load_script(n) for n in glob("../lua/*.lua")}

async def get_db():
    async with ClientSession() as session:
        db = ChClient(session, database='redis', json=orjson) # type: ignore
        assert await db.is_alive()
        try:
            yield db
        finally:
            await db.close()


class Test(BaseModel):
    user_id: int
    username: str
    active: bool
    rate: float
    
class Rows(BaseModel):
    row_count: int


app = FastAPI(default_response_class=ORJSONResponse)

@app.on_event("startup")
async def startup():
    import os
    ID = os.environ["APP_WORKER_ID"]
    if ID == '1':
        from contextlib import asynccontextmanager
        async with asynccontextmanager(get_db)() as db:
            await db.execute("DROP TABLE IF EXISTS test")
            await db.execute("""
                CREATE TABLE IF NOT EXISTS test (
                        user_id UInt32,
                        username String,
                        active Bool,
                        rate Float64
                ) ENGINE=MergeTree()
                PRIMARY KEY (user_id)
            """)
        
        await r.flushall()
        await scripts['schema'](keys=['test', 'user_id', 'username', 'active', 'rate'], args=['UInt32', 'String', 'Bool', 'Float64'])
        max_length = os.environ.get("REDIS_CHUNK_MAX_LEN", 10000)
        await scripts['init'](args=[max_length])


@app.post("/create/", status_code=status.HTTP_201_CREATED)
async def create(test: Test) -> Rows:
    test_data: dict = test.dict()
    test_data['active'] = str(test_data['active']).lower()
    length = await scripts['insert'](keys=list(test_data.keys()), args=list(test_data.values()))
    return Rows(row_count=int(length))

@app.get("/swap/")
async def swap() -> Rows:
    length = await scripts['swap']()
    return Rows(row_count=int(length))

@app.get("/flush/")
async def flush(db: ChClient = Depends(get_db)) -> Rows:
    for chunk in await r.smembers("chunk:id:swap"):
        dump = await scripts['export'](keys=[chunk, 'test'], args=['all'])
        if dump is None:
            return Rows(row_count=0)

        try:
            await db.execute(f"INSERT INTO redis.test FORMAT JSONColumns {dump}")
        except ChClientError as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={"message": str(e)}
            )
        await r.smove("chunk:id:swap", "chunk:id:clean", chunk)
    
    inserted = int(await scripts['clean'](keys=['all']))
    return Rows(row_count=inserted)