"""
connectors.py
--------------
Simulated cloud source/destination connectors for the pipeline
builder. Each connector mimics the interface of a real cloud
service (S3 bucket, GCS bucket, cloud data warehouse table) while
generating or persisting data locally — allowing pipelines built
against this module to be pointed at real cloud SDKs (boto3,
google-cloud-storage, google-cloud-bigquery, snowflake-connector)
without changing pipeline logic.
"""

import os
import numpy as np
import pandas as pd


class S3SourceConnector:
    """Simulates reading a CSV object from an S3-style object store."""

    def __init__(self, bucket: str, key: str):
        self.bucket = bucket
        self.key = key

    def read(self, n_rows: int = 5000, random_state: int = 42) -> pd.DataFrame:
        rng = np.random.default_rng(random_state)
        dates = pd.date_range(end=pd.Timestamp.today().normalize(), periods=n_rows, freq="h")
        df = pd.DataFrame({
            "event_time": dates,
            "device_id": rng.integers(1000, 1050, n_rows),
            "sensor_reading": rng.normal(50, 12, n_rows),
            "status_code": rng.choice([200, 200, 200, 404, 500], n_rows),
        })
        print(f"    [S3SourceConnector] Read {len(df)} rows from s3://{self.bucket}/{self.key}")
        return df


class GCSSourceConnector:
    """Simulates reading a Parquet-style object from GCS."""

    def __init__(self, bucket: str, blob_path: str):
        self.bucket = bucket
        self.blob_path = blob_path

    def read(self, n_rows: int = 4000, random_state: int = 7) -> pd.DataFrame:
        rng = np.random.default_rng(random_state)
        df = pd.DataFrame({
            "order_id": [f"ORD{100000 + i}" for i in range(n_rows)],
            "customer_id": rng.integers(1, 900, n_rows),
            "order_value": rng.lognormal(4.2, 0.6, n_rows).round(2),
            "region": rng.choice(["US", "EU", "APAC", "LATAM"], n_rows, p=[0.45, 0.25, 0.2, 0.1]),
        })
        print(f"    [GCSSourceConnector] Read {len(df)} rows from gs://{self.bucket}/{self.blob_path}")
        return df


class WarehouseDestinationConnector:
    """
    Simulates loading a processed DataFrame into a cloud data
    warehouse table (e.g., BigQuery, Snowflake, Redshift). Writes
    to local disk as a stand-in for a warehouse write, preserving
    the same call signature a real connector would expose.
    """

    def __init__(self, dataset: str, table: str, output_dir: str):
        self.dataset = dataset
        self.table = table
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def write(self, df: pd.DataFrame, mode: str = "overwrite") -> int:
        path = os.path.join(self.output_dir, f"{self.dataset}__{self.table}.csv")
        df.to_csv(path, index=False)
        print(f"    [WarehouseDestinationConnector] Loaded {len(df)} rows into "
              f"{self.dataset}.{self.table} (mode={mode}) -> {path}")
        return len(df)
