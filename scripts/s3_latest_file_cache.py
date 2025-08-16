from datetime import datetime
import os
from typing import Optional
import polars as pl
import boto3
import logging

LOGGER = logging.getLogger(__name__)


class S3LatestFileCache:
    ASOF_SCHEMA = pl.Schema({"time": pl.Datetime("us", "UTC")})
    FOUND_FILES_SCHEMA = pl.Schema(
        {"filename": pl.Utf8, "time": pl.Datetime("us", "UTC"), "search_key": pl.Utf8}
    )

    def __init__(
        self,
        search_lookback: str,
        search_lookback_step: str,
        search_prefix: str,
        file_time_extract: str,
        cache_update_interval: str,
    ):
        self.search_lookback = search_lookback
        self.search_lookback_step = search_lookback_step
        self.search_prefix = search_prefix
        self.file_time_extract = file_time_extract
        self.cache_update_interval = cache_update_interval

        self.last_check_time: Optional[datetime] = None
        self.last_new_file_update_time: Optional[datetime] = None
        self.known_files_df: pl.DataFrame = pl.DataFrame(
            schema=S3LatestFileCache.FOUND_FILES_SCHEMA
        )
        self.prev_file: Optional[str] = None

    def is_stale(self, now: datetime) -> bool:
        if self.last_check_time is None:
            return True

        next_check_time = (
            pl.Series([self.last_check_time]).dt.offset_by(self.cache_update_interval)
        )[0]
        return now > next_check_time

    def _extract_times_from_file_name(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.select(
            filename="Key",
            time=pl.col("Key").str.to_datetime(self.file_time_extract, time_zone="UTC"),
        )

    def _list_files_s3(self, search_key: str) -> pl.DataFrame:
        bucket = os.environ["AWS_BUCKET"]
        s3 = boto3.client("s3")

        response = s3.list_objects_v2(Bucket=bucket, Prefix=search_key)

        if "Contents" in response:
            df = pl.from_records(response["Contents"]).select("Key")
        else:
            df = pl.DataFrame(schema={"Key": pl.Utf8})
        return df

    def _make_search_keys(self, now: datetime) -> list[str]:
        search_keys = (
            pl.DataFrame({"end": now}, schema={"end": pl.Datetime("us", "UTC")})
            .with_columns(start=pl.col("end").dt.offset_by("-" + self.search_lookback))
            .select(
                search_time=pl.datetime_range(
                    pl.col("start").min(),
                    pl.col("end").min(),
                    self.search_lookback_step,
                )
            )
            .select(search_key=pl.col("search_time").dt.strftime(self.search_prefix))
            .sort("search_key", descending=True)
            .to_series()
            .to_list()
        )

        return search_keys

    def _search_latest_file(
        self, search_keys: list[str], now: datetime
    ) -> pl.DataFrame:
        empty_df = pl.DataFrame(schema=S3LatestFileCache.FOUND_FILES_SCHEMA)

        all_dfs: list[pl.DataFrame] = []

        files_with_time_df = empty_df
        for search_key in search_keys:
            files_df = self._list_files_s3(search_key)
            if not files_df.is_empty():
                files_with_time_df = self._extract_times_from_file_name(
                    files_df
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

    def update_cache(self, now: datetime):
        LOGGER.info(f"Updating cache for {now}")
        search_keys = self._make_search_keys(now)
        files_found_df = self._search_latest_file(search_keys, now)
        if not files_found_df.is_empty() and not files_found_df.equals(
            self.known_files_df
        ):
            self.known_files_df = files_found_df
            self.last_new_file_update_time = now
        self.last_check_time = now

        LOGGER.debug(f"Cache updated for {now}")
        LOGGER.debug(f"Files found: {files_found_df}")
        LOGGER.debug(f"Known files: {self.known_files_df}")
        LOGGER.debug(f"Last check time: {self.last_check_time}")
        LOGGER.debug(f"Last new file update time: {self.last_new_file_update_time}")

    def try_get_latest_file(self, now: datetime) -> tuple[bool, Optional[str]]:
        if self.known_files_df.is_empty():
            return False, None

        valid_files = self.known_files_df.filter(pl.col("time") <= now)

        latest_file = valid_files.tail(1).item(0, "filename")

        is_new_file = latest_file != self.prev_file
        if is_new_file:
            self.prev_file = latest_file

        return is_new_file, latest_file
