import polars as pl


# take from: https://stackoverflow.com/a/54314628
def get_all_s3_objects(s3, **base_kwargs):
    continuation_token = None
    while True:
        list_kwargs = dict(MaxKeys=1000, **base_kwargs)
        if continuation_token:
            list_kwargs["ContinuationToken"] = continuation_token
        response = s3.list_objects_v2(**list_kwargs)
        yield from response.get("Contents", [])
        if not response.get("IsTruncated"):  # At the end of the list?
            break
        continuation_token = response.get("NextContinuationToken")


def list_files_s3(bucket: str, search_key: str, s3_client) -> pl.DataFrame:
    all_objects = get_all_s3_objects(s3_client, Bucket=bucket, Prefix=search_key)
    df = pl.DataFrame(all_objects)

    if df.is_empty():
        return pl.DataFrame(schema={"Key": pl.Utf8})

    return df.select("Key")
