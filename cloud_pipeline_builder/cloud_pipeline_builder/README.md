# Cloud Data Pipeline Builder

A lightweight, extensible framework for building and running
DAG-based data pipelines against cloud-style sources and
destinations — simulating the core orchestration engine of a cloud
data platform (comparable in spirit to Airflow / Cloud Composer /
Step Functions), implemented standalone in Python.

## Project Structure

```
cloud_pipeline_builder/
├── main.py                    # Builds and runs an example multi-source pipeline
├── requirements.txt
├── src/
│   ├── pipeline_core.py       # Stage/Pipeline DAG framework + execution engine
│   ├── connectors.py          # Simulated cloud connectors (S3, GCS, warehouse)
│   ├── transformations.py     # Reusable transform functions for stages
│   ├── monitor.py             # Run logging and reporting
│   └── visualize.py           # DAG diagram + run metrics figures
├── data/                      # Pipeline output data (CSV, simulating warehouse loads)
├── logs/                      # JSON run logs per pipeline execution
└── visuals/                   # DAG diagram + execution metrics figures (PNG)
```

## Core Concepts

### `Stage`
A single unit of work (extract / transform / load / quality_check).
Declares its dependencies explicitly:

```python
Stage(
    name="clean_order_data",
    func=lambda ctx: T.clean_order_data(ctx["extract_order_data"]),
    depends_on=["extract_order_data"],
    stage_type="transform",
)
```

### `Pipeline`
A collection of stages forming a DAG. Automatically resolves valid
execution order via topological sort (Kahn's algorithm), detects
cycles, and runs each stage while passing outputs downstream
through a shared `context` dictionary.

```python
pipeline = Pipeline(name="cloud_business_pipeline")
pipeline.add_stage(...).add_stage(...)
pipeline.run()
```

If an upstream stage fails, all downstream dependents are
automatically marked `skipped` rather than executed on incomplete
data.

### Connectors
`connectors.py` provides `S3SourceConnector`, `GCSSourceConnector`,
and `WarehouseDestinationConnector` classes with `.read()` /
`.write()` interfaces matching what a real cloud SDK integration
would expose (boto3, google-cloud-storage, google-cloud-bigquery,
snowflake-connector). Swap the internals for real API calls without
changing any pipeline or stage code.

## Example Pipeline (in `main.py`)

A 9-stage DAG that extracts IoT sensor events (S3) and order data
(GCS) in parallel branches, cleans and aggregates each, joins them
into an enriched regional dataset, runs a data quality check, and
loads the result into a simulated warehouse table:

```
extract_sensor_data -> clean_sensor_data -> aggregate_sensor_hourly ---\
                                                                          -> join_orders_activity -> quality_check_final -> load_to_warehouse
extract_order_data  -> clean_order_data  -> aggregate_orders_by_region -/
```

## Usage

```bash
pip install -r requirements.txt
python main.py
```

Console output shows the resolved execution order, per-stage
success/failure status, duration, and row counts. Outputs are
written to `data/`, `logs/`, and `visuals/`.

## Key Outputs

| File | Description |
|---|---|
| `data/analytics__regional_orders_enriched.csv` | Final loaded dataset |
| `logs/*_run_*.json` | Structured run log (status, duration, rows, errors per stage) |
| `visuals/pipeline_dag.png` | Visual DAG diagram of the pipeline |
| `visuals/stage_durations.png` | Execution time per stage |
| `visuals/stage_row_throughput.png` | Rows processed per stage |

## Extending

- Add new stages with `pipeline.add_stage(Stage(...))`.
- Add new connectors in `connectors.py` following the same
  `.read()` / `.write()` pattern to point at real cloud services.
- Add new transform functions in `transformations.py` — pure
  functions of DataFrame(s) in, DataFrame/dict out.
- Call `pipeline.run(stop_on_failure=False)` to continue past
  failed stages (only their downstream dependents are skipped).
