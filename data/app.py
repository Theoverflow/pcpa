import os
import redis
import httpx
import asyncio
import pandas as pd
from io import StringIO
import redis.asyncio as aioredis


# Configuration via environment
REDIS_HOST        = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT        = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB          = int(os.getenv("REDIS_DB", 0))
REDIS_PASSWORD    = os.getenv("REDIS_PASSWORD", "YourStrongRedisPasswordHere")

UPSTREAM_BASE_URL = os.getenv("UPSTREAM_BASE_URL", "http://localhost:8000")
API_TOKEN         = os.getenv("UPSTREAM_API_TOKEN", "")

CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", 3600))
VERSION_RETRY_COUNT = int(os.getenv("VERSION_RETRY_COUNT", 3))
VERSION_TIMEOUT = float(os.getenv("VERSION_TIMEOUT", 3.0))
CSV_RETRY_COUNT     = int(os.getenv("CSV_RETRY_COUNT", 3))
CSV_TIMEOUT         = float(os.getenv("CSV_TIMEOUT", 30.0))

# Redis client
r = aioredis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    password=REDIS_PASSWORD,
    decode_responses=True,
)


# Cache helpers
# ─── Helpers ─────────────────────────────────────────────────────────────────
async def get_cached_csv(fname: str, version: str):
    key = f"csv:{version}:{fname}"
    return await r.get(key)

async def set_cached_csv(fname:str, version: str, csv_text: str, ttl: int = 3600):
    """Evict old versions, keep only this one, then cache."""
    # Evict any existing keys for this filename
    async for old_key in r.scan_iter(match=f"csv:{version}:*"):
        await r.delete(old_key)
    # Store new version
    key = f"csv:{version}:{fname}"
    await r.set(key, csv_text, ex=ttl)

async def prompt_filename() -> str:
    # offload blocking input() to thread
    return (await asyncio.to_thread(input, "\nEnter filename: ")).strip()


# Upstream helpers

async def fetch_latest_version(fname: str) -> str:
    """
    Fetch latest version with retry on timeouts. Raises RuntimeError after retries.
    """
    url = f"{UPSTREAM_BASE_URL}/file-version"
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    for attempt in range(1, VERSION_RETRY_COUNT + 1):
        try:
            async with httpx.AsyncClient(timeout=VERSION_TIMEOUT) as client:
                resp = await client.get(url, params={"filename_input": fname})
                resp.raise_for_status()
                return resp.json()["version"]
        except (httpx.TimeoutException, httpx.HTTPError) as e:
            if attempt == VERSION_RETRY_COUNT:
                raise RuntimeError(f"Failed to fetch version after {VERSION_RETRY_COUNT} attempts: {e}")
            await asyncio.sleep(1)


async def fetch_csv_payload(version: str) -> str:
    """
    Fetch CSV payload with retry on timeouts. Raises RuntimeError after retries.
    """
    url = f"{UPSTREAM_BASE_URL}/file-csv"
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    params = {"version": version}
    for attempt in range(1, CSV_RETRY_COUNT + 1):
        try:
            async with httpx.AsyncClient(timeout=CSV_TIMEOUT) as client:
                resp = await client.get(url, params=params)
                resp.raise_for_status()
                return resp.json()["csv"]
        except (httpx.TimeoutException, httpx.HTTPError) as e:
            if attempt == CSV_RETRY_COUNT:
                raise RuntimeError(f"Failed to fetch CSV after {CSV_RETRY_COUNT} attempts: {e}")
            await asyncio.sleep(1)


def process_csv_sync(csv_text: str) -> pd.DataFrame:
    """Example processing: load to DataFrame and compute column means."""
    df = pd.read_csv(StringIO(csv_text))
    # simple example: compute mean of numeric columns
    means = df.mean(numeric_only=True).to_frame("mean").reset_index().rename(columns={"index":"column"})
    return means

async def process_csv(csv_text: str) -> pd.DataFrame:
    # offload CPU-bound pandas to thread
    return await asyncio.to_thread(process_csv_sync, csv_text)

def df_to_csv(df: pd.DataFrame) -> str:
    sio = StringIO()
    df.to_csv(sio, index=False)
    return sio.getvalue()


# ─── Main Loop ────────────────────────────────────────────────────────────────
async def main():
    print("Async CSV CLI Processor. Ctrl-C to exit.")
    try:
        while True:
            fname = await prompt_filename()
            if not fname:
                continue
            try:
                version = await fetch_latest_version(fname)
                print(f"Latest version for '{fname}': {version}")
            except httpx.HTTPError as e:
                print(f"[ERROR] fetching version: {e}")
                continue

            # Cache lookup
            cached = await get_cached_csv(fname,version)
            if cached:
                print("Cache hit.")
                csv_text = cached
            else:
                print("Cache miss, fetching CSV…")
                try:
                    csv_text = await fetch_csv_payload(version)
                except httpx.HTTPError as e:
                    print(f"[ERROR] fetching CSV: {e}")
                    continue
                await set_cached_csv(fname, version, csv_text)
                print("Cached new CSV.")

            # Process
            print("Processing…")
            result_df = await process_csv(csv_text)

            # Output CSV result
            output = await asyncio.to_thread(lambda df: df.to_csv(index=False), result_df)
            print("\nProcessed result (CSV):")
            print(output)

    except (KeyboardInterrupt, EOFError):
        print("\nExiting.")

if __name__ == "__main__":
    asyncio.run(main())