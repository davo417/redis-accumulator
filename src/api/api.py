from fastapi import HTTPException
from pydantic import BaseModel
from asyncio import subprocess
import redis.asyncio as redis
from fastapi import FastAPI
from fastapi import status
from pathlib import Path
from uuid import uuid4
from glob import glob


app = FastAPI()
r = redis.Redis(host="localhost", decode_responses=False)

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
    
class Flush(BaseModel):
    row_count: int


async def do_flush() -> int:
    current_lenght = await scripts['length'](keys=['current'])
    if current_lenght == b'0':
        return 0

    await scripts['swap'](args=[str(uuid4())])
    insert = await subprocess.create_subprocess_shell(
        """echo $(redis-cli --eval ../lua/export.lua "all" , "test")"""# + ' | ' +
        # 'clickhouse-client --query="INSERT INTO redis.test FORMAT JSONColumnsWithMetadata"'
    )
    ierr, iout = await insert.communicate()
    if (ierr is not None) and (len(ierr) != 0):
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={'message': ierr}
        )
    cleaned = await scripts['clean']() or 0
    return int(cleaned)


@app.post("/create/", status_code=status.HTTP_201_CREATED)
async def create(test: Test) -> int:
    test: dict = test.dict()
    test['active'] = str(test['active']).lower()
    
    return await scripts['insert'](keys=list(test.keys()), args=list(test.values()))

@app.get("/flush/")
async def flush() -> Flush:
    inserted = await do_flush()
    return Flush(row_count=inserted)