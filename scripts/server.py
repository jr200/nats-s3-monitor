from datetime import datetime, timezone
import logging
import os
from typing import Optional

from fastapi import FastAPI
from pydantic import BaseModel

from s3_latest_file_cache import S3LatestFileCache  # noqa: E402

logging.basicConfig(level=os.getenv("LOG_LEVEL", "WARNING").upper())

LOGGER = logging.getLogger(__name__)

app = FastAPI()

cache: Optional[S3LatestFileCache] = None


class AsofRequest(BaseModel):
    now_since_epoch_ms: int
    search_lookback: str
    search_lookback_step: str
    search_prefix: str
    file_time_extract: str
    cache_update_interval: str


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.get("/asof_utc")
def get_latest(req: AsofRequest):
    LOGGER.debug(f"get_latest_request: {req}")
    global cache

    server_time_utc = datetime.fromtimestamp(
        req.now_since_epoch_ms / 1000, tz=timezone.utc
    )
    if cache is None:
        cache = S3LatestFileCache(
            search_lookback=req.search_lookback,
            search_lookback_step=req.search_lookback_step,
            search_prefix=req.search_prefix,
            file_time_extract=req.file_time_extract,
            cache_update_interval=req.cache_update_interval,
        )

    if cache.is_stale(server_time_utc):
        cache.update_cache(server_time_utc)

    is_new_file, latest_file = cache.try_get_latest_file(server_time_utc)

    return {
        "now": server_time_utc,
        "latest_file": latest_file,
        "is_new_file": is_new_file,
    }
