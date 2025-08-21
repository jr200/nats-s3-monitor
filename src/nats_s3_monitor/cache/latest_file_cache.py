from dataclasses import dataclass, field
from datetime import datetime
import logging
from typing import Any, Optional
import pytz

import polars as pl

from .s3_utils import list_files_s3

LOGGER = logging.getLogger(__name__)


_FOUND_FILES_SCHEMA = pl.Schema(
    {"filename": pl.Utf8, "time": pl.Datetime("us", "UTC"), "search_key": pl.Utf8}  # type: ignore[arg-type]
)

_EMPTY_FILES_DF = pl.DataFrame(schema=_FOUND_FILES_SCHEMA)


@dataclass
class LatestFileCache:
    last_check_time: datetime = datetime.min.replace(tzinfo=pytz.utc)
    last_new_file_update_time: datetime = datetime.min.replace(tzinfo=pytz.utc)
    known_files_df: pl.DataFrame = field(default_factory=lambda: _EMPTY_FILES_DF)
    prev_file: Optional[str] = None

    async def _update_cache_if_stale(
        self, now: datetime, monitor: Any, s3_client: Any
    ) -> bool:
        if not self.is_stale(now, monitor.update_interval):
            return False

        self.update_cache(now, monitor, s3_client)
        return True

    def is_stale(self, now: datetime, update_interval: str) -> bool:
        if self.last_check_time is None:
            return True

        next_check_time = (
            pl.Series([self.last_check_time]).dt.offset_by(update_interval)
        )[0]

        return now > next_check_time

    def update_cache(self, now: datetime, monitor: Any, s3_client: Any):
        LOGGER.info(f"Updating cache for {monitor.id} @ {now.isoformat()}")

        search_keys = _make_search_keys(
            now, monitor.lookback, monitor.lookback_step, monitor.search_prefix
        )
        files_found_df = _search_latest_file(
            monitor.bucket, search_keys, s3_client, monitor.file_time_extract
        )
        if not files_found_df.is_empty() and not files_found_df.equals(
            self.known_files_df
        ):
            self.known_files_df = files_found_df
            self.last_new_file_update_time = now

        self.last_check_time = now

        LOGGER.debug(f"Cache updated for {now}")
        # LOGGER.debug(f"Files found: {files_found_df}")
        LOGGER.debug(f"Known files: {self.known_files_df}")
        LOGGER.debug(f"Last check time: {self.last_check_time}")
        LOGGER.debug(f"Last new file update time: {self.last_new_file_update_time}")

    def try_get_latest_file(self, now: datetime) -> tuple[bool, Optional[str]]:
        if self.known_files_df.is_empty():
            return False, None

        valid_files = self.known_files_df.filter(
            pl.col("time") <= pl.lit(now).cast(pl.dtype_of("time"))
        )
        if valid_files.is_empty():
            return False, None

        latest_file = valid_files.tail(1).item(0, "filename")

        is_new_file = latest_file != self.prev_file
        if is_new_file:
            self.prev_file = latest_file

        return is_new_file, latest_file


def _search_latest_file(
    bucket: str, search_keys: list[str], s3_client, file_time_extract: str
) -> pl.DataFrame:
    empty_df = _EMPTY_FILES_DF

    all_dfs: list[pl.DataFrame] = []

    files_with_time_df = empty_df
    LOGGER.info(f"Searching {bucket} with {len(search_keys)} prefixes...")
    for search_key in search_keys:
        files_df = list_files_s3(bucket, search_key, s3_client)
        if not files_df.is_empty():
            files_with_time_df = _extract_times_from_file_name(
                files_df, file_time_extract
            ).with_columns(search_key=pl.lit(search_key))

            # optimisation TODO
            # keep any rows after 'now'
            # keep the latest row before 'now'

            all_dfs.append(files_with_time_df)
            # exit as soon as we find a historic file
            # this will be the latest file asof 'now'
            break

    if len(all_dfs) > 0:
        result = pl.concat(all_dfs).unique(subset=["filename"]).sort("time")
    else:
        result = empty_df

    return result


def _extract_times_from_file_name(
    df: pl.DataFrame, file_time_extract: str
) -> pl.DataFrame:
    return df.select(
        filename="Key",
        time=pl.col("Key").str.to_datetime(file_time_extract, time_zone="UTC"),
    )


def _make_search_keys(
    now: datetime, search_lookback: str, search_lookback_step: str, search_prefix: str
) -> list[str]:
    search_keys = (
        pl.DataFrame({"end": now}, schema={"end": pl.Datetime("us", "UTC")})
        .with_columns(start=pl.col("end").dt.offset_by("-" + search_lookback))
        .select(
            search_time=pl.datetime_range(
                pl.col("start").min(),
                pl.col("end").min(),
                search_lookback_step,
            )
        )
        .select(search_key=pl.col("search_time").dt.strftime(search_prefix))
        .unique()
        .sort("search_key", descending=True)
        .to_series()
        .to_list()
    )

    return search_keys
