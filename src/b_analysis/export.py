"""Preserve Spark timestamp instants as explicitly offset SGT in local exports."""
import pandas as pd
import pyarrow as pa


def spark_arrow_to_pandas(table):
    frame=table.to_pandas()
    # Spark TimestampType Parquet is UTC-adjusted even if Arrow reports tz=None.
    for field in table.schema:
        if pa.types.is_timestamp(field.type):
            frame[field.name]=pd.to_datetime(frame[field.name],utc=True).dt.tz_convert('Asia/Singapore')
    return frame
