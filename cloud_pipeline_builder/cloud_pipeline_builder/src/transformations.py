"""
transformations.py
--------------------
Reusable transformation functions for pipeline stages. Each
function takes one or more DataFrames and returns a transformed
DataFrame, designed to be composed inside `Stage` callables.
"""

import numpy as np
import pandas as pd


def clean_sensor_data(df: pd.DataFrame) -> pd.DataFrame:
    """Remove failed-status readings and clip physically implausible values."""
    df = df.copy()
    df = df[df["status_code"] == 200]
    df["sensor_reading"] = df["sensor_reading"].clip(lower=0, upper=100)
    return df.drop(columns=["status_code"])


def aggregate_sensor_readings_hourly(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate sensor readings to per-device hourly averages."""
    df = df.copy()
    df["event_hour"] = pd.to_datetime(df["event_time"]).dt.floor("h")
    agg = (
        df.groupby(["device_id", "event_hour"], as_index=False)
        .agg(avg_reading=("sensor_reading", "mean"), reading_count=("sensor_reading", "count"))
    )
    return agg


def clean_order_data(df: pd.DataFrame) -> pd.DataFrame:
    """Remove invalid orders and clip extreme outlier values."""
    df = df.copy()
    df = df[df["order_value"] > 0]
    upper_bound = df["order_value"].quantile(0.995)
    df["order_value"] = df["order_value"].clip(upper=upper_bound)
    return df


def aggregate_orders_by_region(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate order value and count by region."""
    return (
        df.groupby("region", as_index=False)
        .agg(total_order_value=("order_value", "sum"),
             order_count=("order_id", "count"),
             avg_order_value=("order_value", "mean"))
        .sort_values("total_order_value", ascending=False)
    )


def join_orders_with_device_activity(orders_df: pd.DataFrame, sensor_agg_df: pd.DataFrame) -> pd.DataFrame:
    """
    Example cross-domain enrichment join: attaches an aggregate
    device-activity signal to each region as a proxy operational
    load indicator (demonstrates multi-source joins in the DAG).
    """
    device_activity = (
        sensor_agg_df.groupby("device_id", as_index=False)
        .agg(avg_reading=("avg_reading", "mean"))
    )
    activity_index = device_activity["avg_reading"].mean()

    result = orders_df.copy()
    result["operational_activity_index"] = round(activity_index, 2)
    return result


def data_quality_report(df: pd.DataFrame, name: str) -> dict:
    """Generate a simple data quality summary for a DataFrame (used as a DAG stage)."""
    return {
        "dataset": name,
        "row_count": int(len(df)),
        "column_count": int(df.shape[1]),
        "null_counts": {col: int(df[col].isna().sum()) for col in df.columns},
        "duplicate_rows": int(df.duplicated().sum()),
    }
