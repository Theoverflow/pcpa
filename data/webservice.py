import os
import asyncpg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

PG_DSN            = os.getenv("PG_DSN", "postgresql://files_user:StrongP%40ssw0rd@localhost:5432/files_db")
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", 3600))

# PostgreSQL pool placeholder
db_pool: asyncpg.Pool = None

# Pydantic models
class VersionResponse(BaseModel):
    version: str

class CSVResponse(BaseModel):
    version: str
    filename: str
    csv: str


@app.on_event("startup")
async def startup():
    global db_pool
    db_pool = await asyncpg.create_pool(dsn=PG_DSN)

@app.on_event("shutdown")
async def shutdown():
    await db_pool.close()


# Endpoint: GET /file-version -> returns last version and filename
@app.get("/file-version", response_model=VersionResponse)
async def get_file_version(filename_input: str):
    # Fetch latest from file_versions ordered by version_id desc
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT version_label FROM file_versions where filename=$1 ORDER BY version_id DESC LIMIT 1",
            filename_input
        )
        if not row:
            raise HTTPException(status_code=404, detail="No file version found")
        return VersionResponse(version=row["version_label"])


# Endpoint: GET /file-csv?version={version}
@app.get("/file-csv", response_model=CSVResponse)
async def get_file_csv(version: str):
    # First, get filename
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT fv.version_id, fv.filename, sl.gamma, sl.delta, sl.xi, sl.lambda "
            "FROM file_versions fv "
            "JOIN file_data fd ON fd.version_id = fv.version_id "
            "JOIN statistical_limits sl ON sl.version_id = fv.version_id "
            "WHERE fv.version_label = $1",
            version
        )
        if not row:
            raise HTTPException(status_code=404, detail="Version not found")
        v_id, filename, gamma, delta, xi, lambda_ = (
            row["version_id"], row["filename"], row["gamma"], row["delta"], row["xi"], row["lambda"]
        )
    csv=f"{gamma},{delta},{xi},{lambda_}"

    return CSVResponse(
        version=version,
        filename=filename,
        csv=csv
    )
