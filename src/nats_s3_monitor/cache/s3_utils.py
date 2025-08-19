import polars as pl


def list_files_s3(bucket: str, search_key: str, s3_client) -> pl.DataFrame:
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=search_key)

    if "Contents" in response:
        df = pl.from_records(response["Contents"]).select("Key")
    else:
        df = pl.DataFrame(schema={"Key": pl.Utf8})
    return df
