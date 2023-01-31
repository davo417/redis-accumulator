from fastapi import HTTPException
from pydantic import BaseModel
from asyncio import subprocess
import redis.asyncio as redis
from fastapi import FastAPI
from fastapi import status
from pathlib import Path
from glob import glob
import asyncio


app = FastAPI()
r = redis.Redis(host="localhost", decode_responses=True)

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
    
class Rows(BaseModel):
    row_count: int


async def do_export(chunk_id) -> bytes | None:
    insert = await subprocess.create_subprocess_shell(
        f'echo $(redis-cli --eval ../lua/export.lua "{chunk_id}" "test" , "all")' + ' | ' +
        'clickhouse-client --query="INSERT INTO redis.test FORMAT JSONColumnsWithMetadata"'
    )
    ierr, iout = await insert.communicate()
    if (ierr is not None) and (len(ierr) != 0):
        return ierr

    await r.smove("chunk:id:swap", "chunk:id:clean", chunk_id)

async def do_flush() -> int:
    inserts = await asyncio.gather(*[do_export(chunk) for chunk in await r.smembers("chunk:id:swap")])
    inserts = list(filter(lambda ierr: ierr is not None, inserts))
    
    if len(inserts) > 0:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={'messages': inserts}
        )

    return int(await scripts['clean'](keys=['all']))


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
async def flush() -> Rows:
    inserted = await do_flush()
    return Rows(row_count=inserted)