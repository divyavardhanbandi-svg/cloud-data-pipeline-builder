"""
main.py
-------
Cloud Data Pipeline Builder — example pipeline.

Demonstrates building a multi-source, multi-stage DAG pipeline
(extract -> clean -> transform -> quality check -> load) using the
reusable pipeline_core framework, simulated cloud connectors, and
composable transformation functions.

Run:
    python main.py
"""

import os
import sys

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "src"))

from pipeline_core import Pipeline, Stage
from connectors import S3SourceConnector, GCSSourceConnector, WarehouseDestinationConnector
import transformations as T
import monitor
import visualize as viz

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
LOG_DIR = os.path.join(BASE_DIR, "logs")
VISUAL_DIR = os.path.join(BASE_DIR, "visuals")

for d in (DATA_DIR, LOG_DIR, VISUAL_DIR):
    os.makedirs(d, exist_ok=True)


def build_pipeline() -> Pipeline:
    """
    Constructs the example pipeline DAG:

        extract_sensor_data ---> clean_sensor_data ---> aggregate_sensor_hourly ----\\
                                                                                       > join_orders_activity -> quality_check_final -> load_to_warehouse
        extract_order_data  ---> clean_order_data  ---> aggregate_orders_by_region -/
    """
    pipeline = Pipeline(name="cloud_business_pipeline")

    s3_connector = S3SourceConnector(bucket="raw-iot-events", key="sensor_stream/2026/")
    gcs_connector = GCSSourceConnector(bucket="raw-orders-lake", blob_path="orders/2026/")
    warehouse = WarehouseDestinationConnector(dataset="analytics", table="regional_orders_enriched",
                                               output_dir=DATA_DIR)

    pipeline.add_stage(Stage(
        name="extract_sensor_data",
        func=lambda ctx: s3_connector.read(n_rows=6000),
        stage_type="extract",
    ))

    pipeline.add_stage(Stage(
        name="extract_order_data",
        func=lambda ctx: gcs_connector.read(n_rows=4000),
        stage_type="extract",
    ))

    pipeline.add_stage(Stage(
        name="clean_sensor_data",
        func=lambda ctx: T.clean_sensor_data(ctx["extract_sensor_data"]),
        depends_on=["extract_sensor_data"],
        stage_type="transform",
    ))

    pipeline.add_stage(Stage(
        name="clean_order_data",
        func=lambda ctx: T.clean_order_data(ctx["extract_order_data"]),
        depends_on=["extract_order_data"],
        stage_type="transform",
    ))

    pipeline.add_stage(Stage(
        name="aggregate_sensor_hourly",
        func=lambda ctx: T.aggregate_sensor_readings_hourly(ctx["clean_sensor_data"]),
        depends_on=["clean_sensor_data"],
        stage_type="transform",
    ))

    pipeline.add_stage(Stage(
        name="aggregate_orders_by_region",
        func=lambda ctx: T.aggregate_orders_by_region(ctx["clean_order_data"]),
        depends_on=["clean_order_data"],
        stage_type="transform",
    ))

    pipeline.add_stage(Stage(
        name="join_orders_activity",
        func=lambda ctx: T.join_orders_with_device_activity(
            ctx["aggregate_orders_by_region"], ctx["aggregate_sensor_hourly"]
        ),
        depends_on=["aggregate_orders_by_region", "aggregate_sensor_hourly"],
        stage_type="transform",
    ))

    pipeline.add_stage(Stage(
        name="quality_check_final",
        func=lambda ctx: T.data_quality_report(ctx["join_orders_activity"], name="regional_orders_enriched"),
        depends_on=["join_orders_activity"],
        stage_type="quality_check",
    ))

    pipeline.add_stage(Stage(
        name="load_to_warehouse",
        func=lambda ctx: warehouse.write(ctx["join_orders_activity"], mode="overwrite"),
        depends_on=["join_orders_activity", "quality_check_final"],
        stage_type="load",
    ))

    return pipeline


def run_pipeline():
    print("=" * 72)
    print("CLOUD DATA PIPELINE BUILDER — EXAMPLE PIPELINE RUN")
    print("=" * 72)

    pipeline = build_pipeline()

    print(f"\nResolved execution order ({len(pipeline.stages)} stages):")
    for i, stage_name in enumerate(pipeline.execution_order(), 1):
        print(f"    {i}. {stage_name}")

    print("\nExecuting pipeline...\n")
    run_log = pipeline.run(stop_on_failure=True)

    monitor.print_run_report(pipeline.name, run_log)

    log_path = monitor.save_run_log(pipeline.name, run_log, LOG_DIR)
    print(f"\nRun log saved to: {log_path}")

    summary = pipeline.summary()
    print(f"\nSummary: {summary}")

    print("\nGenerating visuals...")
    viz.plot_pipeline_dag(pipeline, os.path.join(VISUAL_DIR, "pipeline_dag.png"))
    viz.plot_run_durations(run_log, os.path.join(VISUAL_DIR, "stage_durations.png"))
    viz.plot_row_throughput(run_log, os.path.join(VISUAL_DIR, "stage_row_throughput.png"))
    print(f"    -> Saved figures to {VISUAL_DIR}")

    print("\n" + "=" * 72)
    print("PIPELINE RUN COMPLETE")
    print("=" * 72)


if __name__ == "__main__":
    run_pipeline()
