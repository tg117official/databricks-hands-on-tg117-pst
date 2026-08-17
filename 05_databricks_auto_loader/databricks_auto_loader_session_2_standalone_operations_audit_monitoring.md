# Auto Loader Production Operations, Audit and Monitoring

This session builds and operates a self-contained multi-entity Auto Loader pipeline for `orders`, `customers`, and `products`. All required setup and execution steps are included:

- Session-specific ADLS folders and checkpoints
- Explicit JSON schemas
- Entity configurations
- Notebook-generated source files
- Auto Loader ingestion code
- Run-level and micro-batch audit tables
- Failure, recovery, monitoring, and file-investigation scenarios
- Serverless Job configuration

## Session index

1. [Create an isolated Session 2 environment](#1-create-an-isolated-session-2-environment)
2. [Define schemas, configurations, and file helpers](#2-define-schemas-configurations-and-file-helpers)
3. [Create the operational audit tables](#3-create-the-operational-audit-tables)
4. [Complete production-style ingestion script](#4-complete-production-style-ingestion-script)
5. [Baseline: create and process the first delivery](#5-baseline-create-and-process-the-first-delivery)
6. [Scenario 1: burst arrival and rate control](#6-scenario-1-burst-arrival-and-rate-control)
7. [Scenario 2: uneven entity workloads](#7-scenario-2-uneven-entity-workloads)
8. [Scenario 3: one entity fails](#8-scenario-3-one-entity-fails)
9. [Scenario 4: targeted recovery](#9-scenario-4-targeted-recovery)
10. [Monitor runs, batches, and file state](#10-monitor-runs-batches-and-file-state)
11. [Scenario 5: investigate a reported file](#11-scenario-5-investigate-a-reported-file)
12. [Create the dedicated job notebook](#12-create-the-dedicated-job-notebook)
13. [Create the Serverless Job](#13-create-the-serverless-job)
14. [Scenario 6: a second run is requested](#14-scenario-6-a-second-run-is-requested)
15. [Final job-ready execution](#15-final-job-ready-execution)
16. [Recovery checklist](#16-recovery-checklist)

---

## 1. Create an isolated Session 2 environment

All ADLS paths use:

```text
abfss://data@demodb117.dfs.core.windows.net/autoloader/production_session_2/
```

The External Location governing this path must allow the notebook or job identity to read landing files and write checkpoint state.

The execution identity also needs permission to use `auto_loader_demo.sales_data`, create the session tables, and modify those tables during ingestion and auditing.

### 1.1 Create the catalog schema

```sql
CREATE SCHEMA IF NOT EXISTS auto_loader_demo.sales_data;

USE CATALOG auto_loader_demo;
USE SCHEMA sales_data;
```

### 1.2 Optional clean start

Run these cells only when resetting this isolated learning exercise. Do not use checkpoint deletion as a normal production-recovery method.

```sql
DROP TABLE IF EXISTS auto_loader_demo.sales_data.orders_bronze_operations;
DROP TABLE IF EXISTS auto_loader_demo.sales_data.customers_bronze_operations;
DROP TABLE IF EXISTS auto_loader_demo.sales_data.products_bronze_operations;
DROP TABLE IF EXISTS auto_loader_demo.sales_data.auto_loader_run_audit_operations;
DROP TABLE IF EXISTS auto_loader_demo.sales_data.auto_loader_batch_audit_operations;
```

```python
dbutils.fs.rm(
    "abfss://data@demodb117.dfs.core.windows.net/"
    "autoloader/production_session_2/",
    True
)
```

### Operational considerations

- This reset is safe only because the pipeline uses an isolated practice directory.
- Deleting a checkpoint removes the stream's file-processing history.
- Production checkpoint directories should not be covered by storage lifecycle rules that delete checkpoint files.

---

## 2. Define schemas, configurations, and file helpers

### 2.1 Define the three JSON schemas

```python
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DoubleType,
    TimestampType
)


orders_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("order_timestamp", TimestampType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])

customers_schema = StructType([
    StructField("customer_id", LongType(), True),
    StructField("customer_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("email", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])

products_schema = StructType([
    StructField("product_id", LongType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("_corrupt_record", StringType(), True)
])
```

The pipeline uses the following parsing policy:

| Input condition | Treatment |
| --- | --- |
| Expected field and datatype | Parsed into the declared column |
| Unexpected field or incompatible datatype | Captured in `_rescued_data` |
| Malformed JSON | Captured in `_corrupt_record` |

### 2.2 Create independent entity configurations

```python
entity_configs = {
    "orders": {
        "schema": orders_schema,
        "source_path": (
            "abfss://data@demodb117.dfs.core.windows.net/"
            "autoloader/production_session_2/landing/orders/"
        ),
        "checkpoint_path": (
            "abfss://data@demodb117.dfs.core.windows.net/"
            "autoloader/production_session_2/checkpoints/orders/"
        ),
        "target_table": (
            "auto_loader_demo.sales_data.orders_bronze_operations"
        )
    },
    "customers": {
        "schema": customers_schema,
        "source_path": (
            "abfss://data@demodb117.dfs.core.windows.net/"
            "autoloader/production_session_2/landing/customers/"
        ),
        "checkpoint_path": (
            "abfss://data@demodb117.dfs.core.windows.net/"
            "autoloader/production_session_2/checkpoints/customers/"
        ),
        "target_table": (
            "auto_loader_demo.sales_data.customers_bronze_operations"
        )
    },
    "products": {
        "schema": products_schema,
        "source_path": (
            "abfss://data@demodb117.dfs.core.windows.net/"
            "autoloader/production_session_2/landing/products/"
        ),
        "checkpoint_path": (
            "abfss://data@demodb117.dfs.core.windows.net/"
            "autoloader/production_session_2/checkpoints/products/"
        ),
        "target_table": (
            "auto_loader_demo.sales_data.products_bronze_operations"
        )
    }
}
```

Every entity has its own source, target, and checkpoint.

### 2.3 Validate the configuration

```python
# Purpose: Check that every entity has all required settings and its own
# source, checkpoint, and target. This catches configuration mistakes early.
def validate_entity_configs(configs):
    required_keys = {
        "schema",
        "source_path",
        "checkpoint_path",
        "target_table"
    }

    for entity_name, config in configs.items():
        missing_keys = required_keys - set(config)
        if missing_keys:
            raise ValueError(
                f"{entity_name} is missing {sorted(missing_keys)}"
            )

    for key in [
        "source_path",
        "checkpoint_path",
        "target_table"
    ]:
        values = [config[key] for config in configs.values()]
        if len(values) != len(set(values)):
            raise ValueError(
                f"Every entity requires a unique {key}."
            )

    print("Configuration validation passed.")


validate_entity_configs(entity_configs)
```

### 2.4 Create landing folders and the JSON writer

```python
import json
from datetime import datetime, timezone


for config in entity_configs.values():
    dbutils.fs.mkdirs(config["source_path"])


# Purpose: Create a newline-delimited JSON delivery directly in ADLS so no
# external text editor or manual upload is required.
def write_json_records(file_path, records):
    file_content = "\n".join(
        json.dumps(record, separators=(",", ":"))
        for record in records
    )

    dbutils.fs.put(
        file_path,
        file_content + "\n",
        overwrite=False
    )


delivery_batch_tag = datetime.now(timezone.utc).strftime(
    "%Y%m%d%H%M%S"
)

print(f"Delivery batch tag: {delivery_batch_tag}")
```

`overwrite=False` prevents the notebook from silently replacing an existing delivery. The generated files use newline-delimited JSON: one JSON object per line.

### Operational considerations

- `dbutils.fs.put()` simulates source delivery for this environment; it is not the production client-delivery mechanism.
- A production landing zone should normally treat delivered files as immutable.
- `READ FILES` on landing data does not automatically provide permission to write checkpoint files.

---

## 3. Create the operational audit tables

```sql
CREATE TABLE IF NOT EXISTS
auto_loader_demo.sales_data.auto_loader_run_audit_operations (
    pipeline_run_id STRING,
    entity_name STRING,
    query_name STRING,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    status STRING,
    run_outcome STRING,
    files_written BIGINT,
    rows_written BIGINT,
    micro_batches BIGINT,
    duration_seconds DOUBLE,
    error_message STRING
)
USING DELTA;

CREATE TABLE IF NOT EXISTS
auto_loader_demo.sales_data.auto_loader_batch_audit_operations (
    pipeline_run_id STRING,
    entity_name STRING,
    query_name STRING,
    progress_timestamp STRING,
    batch_id BIGINT,
    input_rows BIGINT,
    input_rows_per_second DOUBLE,
    processed_rows_per_second DOUBLE,
    files_outstanding BIGINT,
    bytes_outstanding BIGINT,
    latest_offset_ms BIGINT,
    add_batch_ms BIGINT
)
USING DELTA;
```

The run table stores the outcome of each entity. The batch table preserves Structured Streaming progress for operational monitoring.

---

## 4. Complete production-style ingestion script

Run this complete script before breaking it down through the scenarios.

```python
import uuid
from datetime import datetime, timezone

from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType,
    DoubleType
)


RUN_AUDIT_TABLE = (
    "auto_loader_demo.sales_data."
    "auto_loader_run_audit_operations"
)
BATCH_AUDIT_TABLE = (
    "auto_loader_demo.sales_data."
    "auto_loader_batch_audit_operations"
)


run_audit_schema = StructType([
    StructField("pipeline_run_id", StringType(), False),
    StructField("entity_name", StringType(), False),
    StructField("query_name", StringType(), False),
    StructField("started_at", TimestampType(), False),
    StructField("ended_at", TimestampType(), False),
    StructField("status", StringType(), False),
    StructField("run_outcome", StringType(), False),
    StructField("files_written", LongType(), False),
    StructField("rows_written", LongType(), False),
    StructField("micro_batches", LongType(), False),
    StructField("duration_seconds", DoubleType(), False),
    StructField("error_message", StringType(), True)
])


batch_audit_schema = StructType([
    StructField("pipeline_run_id", StringType(), False),
    StructField("entity_name", StringType(), False),
    StructField("query_name", StringType(), False),
    StructField("progress_timestamp", StringType(), True),
    StructField("batch_id", LongType(), False),
    StructField("input_rows", LongType(), False),
    StructField("input_rows_per_second", DoubleType(), False),
    StructField("processed_rows_per_second", DoubleType(), False),
    StructField("files_outstanding", LongType(), True),
    StructField("bytes_outstanding", LongType(), True),
    StructField("latest_offset_ms", LongType(), True),
    StructField("add_batch_ms", LongType(), True)
])


# Purpose: Convert a reported metric to an integer while preserving missing
# metrics as None.
def optional_int(value):
    return None if value is None else int(value)


# Purpose: Convert a metric to an integer and use the supplied default when
# Databricks reports the metric as None.
def safe_int(value, default=0):
    return default if value is None else int(value)


# Purpose: Convert a reported rate to a decimal number and use 0.0 when the
# metric is not available.
def safe_float(value):
    return 0.0 if value is None else float(value)


# Purpose: Build the Auto Loader DataFrame for one entity using its source,
# schema, rate limit, and standard ingestion metadata.
def build_entity_stream(
    entity_name,
    config,
    max_files_per_trigger
):
    source_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option(
            "cloudFiles.maxFilesPerTrigger",
            max_files_per_trigger
        )
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .option("readerCaseSensitive", "true")
        .option("mode", "PERMISSIVE")
        .option(
            "columnNameOfCorruptRecord",
            "_corrupt_record"
        )
        .schema(config["schema"])
        .load(config["source_path"])
    )

    return (
        source_df
        .withColumn("entity_name", lit(entity_name))
        .withColumn(
            "source_file_path",
            col("_metadata.file_path")
        )
        .withColumn(
            "source_file_name",
            col("_metadata.file_name")
        )
        .withColumn(
            "source_file_modified_at",
            col("_metadata.file_modification_time")
        )
        .withColumn("ingested_at", current_timestamp())
    )


# Purpose: Convert Structured Streaming progress events into simple rows that
# can be stored in the batch-audit Delta table.
def extract_batch_rows(
    pipeline_run_id,
    entity_name,
    query_name,
    progress_events
):
    batch_rows = []

    for progress in progress_events:
        sources = progress.get("sources", [])
        source = sources[0] if sources else {}
        metrics = source.get("metrics", {}) or {}
        durations = progress.get("durationMs", {}) or {}

        # Row and rate metrics can be reported at source level. Fall back to
        # the top-level progress values when the source value is unavailable.
        input_rows_value = source.get("numInputRows")
        if input_rows_value is None:
            input_rows_value = progress.get("numInputRows")

        input_rate_value = source.get("inputRowsPerSecond")
        if input_rate_value is None:
            input_rate_value = progress.get("inputRowsPerSecond")

        processed_rate_value = source.get(
            "processedRowsPerSecond"
        )
        if processed_rate_value is None:
            processed_rate_value = progress.get(
                "processedRowsPerSecond"
            )

        batch_rows.append({
            "pipeline_run_id": pipeline_run_id,
            "entity_name": entity_name,
            "query_name": query_name,
            "progress_timestamp": progress.get("timestamp"),
            "batch_id": safe_int(
                progress.get("batchId"),
                -1
            ),
            "input_rows": safe_int(
                input_rows_value,
                0
            ),
            "input_rows_per_second": safe_float(
                input_rate_value
            ),
            "processed_rows_per_second": safe_float(
                processed_rate_value
            ),
            "files_outstanding": optional_int(
                metrics.get("numFilesOutstanding")
            ),
            "bytes_outstanding": optional_int(
                metrics.get("numBytesOutstanding")
            ),
            "latest_offset_ms": optional_int(
                durations.get("latestOffset")
            ),
            "add_batch_ms": optional_int(
                durations.get("addBatch")
            )
        })

    return batch_rows


# Purpose: Count the rows and distinct source files written during the current
# entity run. This provides easy-to-read audit totals for the exercise.
def count_written_rows_and_files(
    target_table,
    started_at,
    ended_at
):
    rows_from_run = (
        spark.table(target_table)
        .where(
            (col("ingested_at") >= lit(started_at))
            & (col("ingested_at") <= lit(ended_at))
        )
    )

    rows_written = rows_from_run.count()
    files_written = (
        rows_from_run
        .select("source_file_path")
        .distinct()
        .count()
    )

    return rows_written, files_written


# Purpose: Append one final entity result to the run-audit Delta table.
def save_run_audit(record):
    (
        spark.createDataFrame([record], run_audit_schema)
        .write
        .mode("append")
        .saveAsTable(RUN_AUDIT_TABLE)
    )


# Purpose: Append the available micro-batch progress records to the batch-audit
# Delta table. A no-data run might not produce any records.
def save_batch_audit(records):
    if records:
        (
            spark.createDataFrame(records, batch_audit_schema)
            .write
            .mode("append")
            .saveAsTable(BATCH_AUDIT_TABLE)
        )


# Purpose: Run one entity from start to finish, capture its progress, record its
# success or failure, and return its audit result.
def run_entity(
    pipeline_run_id,
    entity_name,
    config,
    max_files_per_trigger=2
):
    started_at = datetime.now(timezone.utc)
    query_name = (
        f"autoloader_{entity_name}_"
        f"{pipeline_run_id.replace('-', '_')}"
    )
    query = None
    progress_events = []
    status = "SUCCESS"
    error_message = None

    try:
        entity_df = build_entity_stream(
            entity_name,
            config,
            max_files_per_trigger
        )

        query = (
            entity_df.writeStream
            .format("delta")
            .outputMode("append")
            .queryName(query_name)
            .option(
                "checkpointLocation",
                config["checkpoint_path"]
            )
            .trigger(availableNow=True)
            .toTable(config["target_table"])
        )

        query.awaitTermination()
        progress_events = list(query.recentProgress)

    except Exception as error:
        status = "FAILED"
        error_message = str(error)[:4000]

        if query is not None:
            progress_events = list(query.recentProgress)

    ended_at = datetime.now(timezone.utc)

    try:
        rows_written, files_written = count_written_rows_and_files(
            config["target_table"],
            started_at,
            ended_at
        )
    except Exception:
        rows_written = 0
        files_written = 0

    batch_rows = extract_batch_rows(
        pipeline_run_id,
        entity_name,
        query_name,
        progress_events
    )
    save_batch_audit(batch_rows)

    run_outcome = (
        "FAILED"
        if status == "FAILED"
        else "NO_DATA"
        if rows_written == 0
        else "PROCESSED"
    )

    run_record = {
        "pipeline_run_id": pipeline_run_id,
        "entity_name": entity_name,
        "query_name": query_name,
        "started_at": started_at,
        "ended_at": ended_at,
        "status": status,
        "run_outcome": run_outcome,
        "files_written": int(files_written),
        "rows_written": int(rows_written),
        "micro_batches": int(len(progress_events)),
        "duration_seconds": float(
            (ended_at - started_at).total_seconds()
        ),
        "error_message": error_message
    }
    save_run_audit(run_record)

    print(
        f"{entity_name}: status={status}, "
        f"outcome={run_outcome}, "
        f"files={files_written}, "
        f"rows={rows_written}, "
        f"batches={len(progress_events)}"
    )

    return run_record


# Purpose: Create one pipeline run ID, run the selected entities, and fail the
# overall job after auditing when any entity has failed.
def run_entities(
    configs,
    selected_entities=None,
    max_files_per_trigger=2,
    fail_job_on_entity_error=True
):
    pipeline_run_id = str(uuid.uuid4())
    entities = (
        list(configs)
        if selected_entities is None
        else selected_entities
    )

    results = []

    for entity_name in entities:
        result = run_entity(
            pipeline_run_id,
            entity_name,
            configs[entity_name],
            max_files_per_trigger
        )
        results.append(result)

    failed_entities = [
        result["entity_name"]
        for result in results
        if result["status"] == "FAILED"
    ]

    print(f"Pipeline run ID: {pipeline_run_id}")

    if failed_entities and fail_job_on_entity_error:
        raise RuntimeError(
            "Failed entities: " + ", ".join(failed_entities)
        )

    return pipeline_run_id
```

### Execution order

```text
Create one pipeline run ID
        ↓
Run each selected entity
        ↓
AvailableNow processes pending files
        ↓
Capture progress for every micro-batch
        ↓
Write batch and entity audits
        ↓
Fail the job when an entity failed
```

### Operational considerations

- `cloudFiles.maxFilesPerTrigger` belongs on `readStream`, not `writeStream`.
- It limits one micro-batch; it does not limit the entire `AvailableNow` run.
- `awaitTermination()` waits for an already-started query.
- A caught exception must still be propagated at job level after its audit record is saved.
- The ingestion-time scan keeps the example compact. Large production tables should use purpose-built metrics rather than repeated full-table scans.

### If a null progress metric caused an earlier failure

An `AvailableNow` query can complete its data write before later audit-processing code fails. Check the Bronze tables before repeating file generation. After correcting `safe_int()`, rerunning with the same checkpoint processes only work that is still pending.

For a completely fresh baseline result, use the optional clean-start cells and then rerun the document from the beginning. Use that reset only for this isolated practice environment.

---

## 5. Baseline: create and process the first delivery

The baseline creates every Bronze table and confirms that the standalone framework works before operational incidents are introduced.

### Cell 1 — Generate one file for each entity

```python
write_json_records(
    entity_configs["orders"]["source_path"]
    + f"orders_baseline_{delivery_batch_tag}_001.json",
    [
        {
            "order_id": 3001,
            "customer_id": 1001,
            "order_timestamp": "2026-08-17T09:01:00Z",
            "amount": 2499.0,
            "status": "CONFIRMED"
        },
        {
            "order_id": 3002,
            "customer_id": 1002,
            "order_timestamp": "2026-08-17T09:02:00Z",
            "amount": 799.0,
            "status": "SHIPPED"
        }
    ]
)

write_json_records(
    entity_configs["customers"]["source_path"]
    + f"customers_baseline_{delivery_batch_tag}_001.json",
    [
        {
            "customer_id": 1001,
            "customer_name": "Aarav Sharma",
            "city": "Pune",
            "email": "aarav@example.com"
        },
        {
            "customer_id": 1002,
            "customer_name": "Priya Deshmukh",
            "city": "Mumbai",
            "email": "priya@example.com"
        }
    ]
)

write_json_records(
    entity_configs["products"]["source_path"]
    + f"products_baseline_{delivery_batch_tag}_001.json",
    [
        {
            "product_id": 1101,
            "product_name": "Mechanical Keyboard",
            "category": "Accessories",
            "price": 2499.0
        },
        {
            "product_id": 1102,
            "product_name": "Wireless Mouse",
            "category": "Accessories",
            "price": 799.0
        }
    ]
)
```

### Cell 2 — Process the baseline

```python
baseline_run_id = run_entities(
    entity_configs,
    max_files_per_trigger=2
)
```

### Cell 3 — Validate the baseline audit

```python
display(
    spark.table(RUN_AUDIT_TABLE)
    .where(col("pipeline_run_id") == baseline_run_id)
    .select(
        "entity_name",
        "status",
        "run_outcome",
        "files_written",
        "rows_written"
    )
    .orderBy("entity_name")
)
```

Expected result: every entity has `SUCCESS`, `PROCESSED`, one file, and two rows.

---

## 6. Scenario 1: burst arrival and rate control

### Problem

Seven order files arrive together. The pipeline should divide them into smaller micro-batches while still draining the complete pending workload.

### Cell 1 — Generate seven order files

```python
for file_number in range(1, 8):
    write_json_records(
        entity_configs["orders"]["source_path"]
        + f"orders_burst_{delivery_batch_tag}_{file_number:03d}.json",
        [
            {
                "order_id": 3100 + file_number,
                "customer_id": 1200 + file_number,
                "order_timestamp": (
                    f"2026-08-17T10:{file_number:02d}:00Z"
                ),
                "amount": float(500 + file_number * 100),
                "status": "CONFIRMED"
            }
        ]
    )
```

### Cell 2 — Process two files per micro-batch

```python
burst_run_id = run_entities(
    entity_configs,
    selected_entities=["orders"],
    max_files_per_trigger=2
)
```

### Cell 3 — Inspect the run and micro-batches

```python
display(
    spark.table(RUN_AUDIT_TABLE)
    .where(col("pipeline_run_id") == burst_run_id)
)

display(
    spark.table(BATCH_AUDIT_TABLE)
    .where(col("pipeline_run_id") == burst_run_id)
    .orderBy("batch_id")
)
```

Expected observations:

- Seven files are written to Bronze.
- Multiple micro-batches are visible.
- `files_outstanding` should reduce as the run progresses when the metric is reported.
- The query stops only after the available workload is drained.
- Because every file contains one row and the file limit is two, the data batches normally report input-row counts such as `2, 2, 2, 1`, with a total of seven. Exact progress-event details can vary by runtime.
- If row or rate telemetry is unavailable at both source and query level, the audit uses zero as a safe fallback. Confirm the actual write using `rows_written` in the run audit and the Bronze table.

Existing audit rows are historical and are not recalculated after the extraction code changes. To test corrected metrics, create a new `delivery_batch_tag`, generate another burst, and run the scenario again.

### Operational considerations

- The file limit is hard, but files can contain very different numbers of rows and bytes.
- `cloudFiles.maxBytesPerTrigger` is a soft limit and can be discussed as an alternative for uneven file sizes.
- A very small limit can lengthen the run and contribute to a growing backlog.

---

## 7. Scenario 2: uneven entity workloads

### Problem

Orders receive five files, customers receive two files, and products receive no new file.

### Cell 1 — Generate the uneven delivery

```python
for file_number in range(1, 6):
    write_json_records(
        entity_configs["orders"]["source_path"]
        + f"orders_uneven_{delivery_batch_tag}_{file_number:03d}.json",
        [
            {
                "order_id": 3200 + file_number,
                "customer_id": 1300 + file_number,
                "order_timestamp": (
                    f"2026-08-17T11:{file_number:02d}:00Z"
                ),
                "amount": float(1000 + file_number * 50),
                "status": "CONFIRMED"
            }
        ]
    )

for file_number in range(1, 3):
    write_json_records(
        entity_configs["customers"]["source_path"]
        + f"customers_uneven_{delivery_batch_tag}_{file_number:03d}.json",
        [
            {
                "customer_id": 1300 + file_number,
                "customer_name": f"Operations Customer {file_number}",
                "city": "Pune",
                "email": f"operations{file_number}@example.com"
            }
        ]
    )
```

No product file is created.

### Cell 2 — Run every entity

```python
uneven_run_id = run_entities(
    entity_configs,
    max_files_per_trigger=2
)
```

### Cell 3 — Compare entity outcomes

```python
display(
    spark.table(RUN_AUDIT_TABLE)
    .where(col("pipeline_run_id") == uneven_run_id)
    .select(
        "entity_name",
        "status",
        "run_outcome",
        "files_written",
        "rows_written",
        "micro_batches",
        "duration_seconds"
    )
    .orderBy("entity_name")
)
```

Expected result:

| Entity | Status | Outcome | Files | Rows |
| --- | --- | --- | ---: | ---: |
| Orders | `SUCCESS` | `PROCESSED` | 5 | 5 |
| Customers | `SUCCESS` | `PROCESSED` | 2 | 2 |
| Products | `SUCCESS` | `NO_DATA` | 0 | 0 |

### Operational considerations

- `NO_DATA` is not a technical failure.
- It can still represent an SLA breach when a product delivery was expected.
- Per-entity audit rows reveal volume imbalance that one overall job status hides.

---

## 8. Scenario 3: one entity fails

### Problem

Orders use an invalid target configuration. Customers and products should still leave visible audit results.

### Cell 1 — Generate one file for each entity

```python
write_json_records(
    entity_configs["orders"]["source_path"]
    + f"orders_failure_{delivery_batch_tag}_001.json",
    [
        {
            "order_id": 3301,
            "customer_id": 1401,
            "order_timestamp": "2026-08-17T12:01:00Z",
            "amount": 2200.0,
            "status": "CONFIRMED"
        }
    ]
)

write_json_records(
    entity_configs["customers"]["source_path"]
    + f"customers_failure_{delivery_batch_tag}_001.json",
    [
        {
            "customer_id": 1401,
            "customer_name": "Failure Test Customer",
            "city": "Mumbai",
            "email": "failure.test@example.com"
        }
    ]
)

write_json_records(
    entity_configs["products"]["source_path"]
    + f"products_failure_{delivery_batch_tag}_001.json",
    [
        {
            "product_id": 1401,
            "product_name": "Failure Test Product",
            "category": "Operations",
            "price": 2200.0
        }
    ]
)
```

### Cell 2 — Introduce an invalid orders target

```python
import copy


broken_configs = copy.deepcopy(entity_configs)
broken_configs["orders"]["target_table"] = (
    "auto_loader_demo.schema_does_not_exist."
    "orders_bronze_operations"
)
```

### Cell 3 — Run all entities and retain the audit results

```python
failure_run_id = run_entities(
    broken_configs,
    max_files_per_trigger=2,
    fail_job_on_entity_error=False
)
```

`fail_job_on_entity_error=False` allows all entity results to be inspected without stopping the cell.

### Cell 4 — Inspect the partial outcome

```python
display(
    spark.table(RUN_AUDIT_TABLE)
    .where(col("pipeline_run_id") == failure_run_id)
    .select(
        "entity_name",
        "status",
        "run_outcome",
        "files_written",
        "rows_written",
        "error_message"
    )
    .orderBy("entity_name")
)
```

Expected result:

- Orders: `FAILED`
- Customers: `SUCCESS`
- Products: `SUCCESS`

### Operational considerations

- A configuration failure is different from a corrupt source record.
- Recording an exception is not enough; a production job must also finish with a failed task status.
- Error messages may expose sensitive values and should be sanitized before long-term retention.

---

## 9. Scenario 4: targeted recovery

### Problem

Only orders failed. Correct the configuration and run only the affected entity.

### Cell 1 — Recover orders using the original checkpoint

```python
recovery_run_id = run_entities(
    entity_configs,
    selected_entities=["orders"],
    max_files_per_trigger=2
)
```

### Cell 2 — Validate the recovery

```python
display(
    spark.table(RUN_AUDIT_TABLE)
    .where(col("pipeline_run_id") == recovery_run_id)
)

display(
    spark.table(
        "auto_loader_demo.sales_data.orders_bronze_operations"
    )
    .where(
        col("source_file_name")
        == f"orders_failure_{delivery_batch_tag}_001.json"
    )
    .select(
        "order_id",
        "source_file_name",
        "ingested_at"
    )
)
```

### Operational considerations

- Correct the root cause before retrying.
- Preserve the checkpoint so Auto Loader can continue from its known state.
- Targeted recovery avoids rerunning unrelated entities.
- Never delete a production checkpoint as the first recovery step.

---

## 10. Monitor runs, batches, and file state

### 10.1 Entity-level run history

```sql
SELECT
    pipeline_run_id,
    entity_name,
    status,
    run_outcome,
    files_written,
    rows_written,
    micro_batches,
    ROUND(duration_seconds, 2) AS duration_seconds,
    error_message
FROM auto_loader_demo.sales_data.auto_loader_run_audit_operations
ORDER BY started_at DESC, entity_name;
```

### 10.2 Micro-batch and backlog metrics

```sql
SELECT
    pipeline_run_id,
    entity_name,
    batch_id,
    input_rows,
    files_outstanding,
    bytes_outstanding,
    ROUND(processed_rows_per_second, 2)
        AS processed_rows_per_second,
    latest_offset_ms,
    add_batch_ms
FROM auto_loader_demo.sales_data.auto_loader_batch_audit_operations
ORDER BY progress_timestamp DESC, entity_name, batch_id;
```

| Observation | Possible meaning |
| --- | --- |
| `files_outstanding` decreases | The run is reducing the backlog |
| Outstanding files grow across runs | Files arrive faster than they are processed |
| Large `latest_offset_ms` | File discovery may be slow |
| Large `add_batch_ms` | Transformation or sink processing may be slow |

### 10.3 Orders file state

```sql
SELECT
    path,
    size,
    create_time,
    discovery_time,
    processed_time,
    commit_time,
    ingestion_state
FROM cloud_files_state(
    'abfss://data@demodb117.dfs.core.windows.net/autoloader/production_session_2/checkpoints/orders/'
)
ORDER BY create_time DESC;
```

### 10.4 Customers file state

```sql
SELECT
    path,
    size,
    create_time,
    discovery_time,
    processed_time,
    commit_time,
    ingestion_state
FROM cloud_files_state(
    'abfss://data@demodb117.dfs.core.windows.net/autoloader/production_session_2/checkpoints/customers/'
)
ORDER BY create_time DESC;
```

### 10.5 Products file state

```sql
SELECT
    path,
    size,
    create_time,
    discovery_time,
    processed_time,
    commit_time,
    ingestion_state
FROM cloud_files_state(
    'abfss://data@demodb117.dfs.core.windows.net/autoloader/production_session_2/checkpoints/products/'
)
ORDER BY create_time DESC;
```

Some file-state timestamps depend on the Databricks Runtime and Auto Loader configuration. If a timestamp column is unavailable, begin with `SELECT *` and inspect the fields provided by the current environment.

Find orders files that are not committed:

```sql
SELECT
    path,
    size,
    ingestion_state
FROM cloud_files_state(
    'abfss://data@demodb117.dfs.core.windows.net/autoloader/production_session_2/checkpoints/orders/'
)
WHERE ingestion_state != 'COMMITTED';
```

---

## 11. Scenario 5: investigate a reported file

### Problem

An operations ticket asks whether the first burst file was received and processed.

### Cell 1 — Define the reported filename

```python
reported_file_name = (
    f"orders_burst_{delivery_batch_tag}_001.json"
)
print(reported_file_name)
```

### Cell 2 — Check the landing folder

```python
landing_matches = [
    file_info
    for file_info in dbutils.fs.ls(
        entity_configs["orders"]["source_path"]
    )
    if file_info.name == reported_file_name
]

display(landing_matches)
```

### Cell 3 — Check the checkpoint state

```python
orders_checkpoint = entity_configs["orders"]["checkpoint_path"]

display(
    spark.sql(
        f"""
        SELECT *
        FROM cloud_files_state('{orders_checkpoint}')
        WHERE path LIKE '%{reported_file_name}'
        """
    )
)
```

### Cell 4 — Check Bronze metadata

```python
display(
    spark.table(
        "auto_loader_demo.sales_data.orders_bronze_operations"
    )
    .where(col("source_file_name") == reported_file_name)
    .select(
        "order_id",
        "source_file_name",
        "source_file_modified_at",
        "ingested_at",
        "_rescued_data",
        "_corrupt_record"
    )
)
```

### Cell 5 — Check the related entity audit

```python
display(
    spark.table(RUN_AUDIT_TABLE)
    .where(col("pipeline_run_id") == burst_run_id)
    .where(col("entity_name") == "orders")
)
```

### Classify the result

| Evidence | Interpretation |
| --- | --- |
| Absent from landing | Undelivered, moved, or removed by retention |
| In landing but absent from file state | Not discovered yet, or wrong checkpoint inspected |
| File state is not `COMMITTED` | Pending, skipped, or affected by failure |
| `COMMITTED` and present in Bronze | File ingestion completed |
| Bronze row has rescued or corrupt content | File committed, but record quality needs investigation |

### Blind point to discuss

File arrival, checkpoint commit, Bronze ingestion, and record quality are separate operational questions.

---

## 12. Create the dedicated job notebook

The notebook used for explanations and scenarios contains optional reset cells, test-data generation, inspection queries, and intentional failures. A scheduled job should not execute those cells.

Create a separate notebook containing only the stable ingestion code.

### 12.1 Create the notebook

1. Open **Workspace**.
2. Select the folder where the production notebook should be stored.
3. Select **Create** → **Notebook**.
4. Name the notebook `auto_loader_multi_table_job`.
5. Select **Python** as the default language.
6. Paste the complete code below into one Python cell.
7. Save the notebook.

Using one complete cell prevents the job from depending on the order or numbering of cells in the scenario notebook.

### 12.2 Complete code for `auto_loader_multi_table_job`

```python
import uuid
from datetime import datetime, timezone

from pyspark.sql.functions import col, current_timestamp, lit
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType,
    DoubleType
)


# Job parameters make the same notebook usable for all entities or a selected
# recovery scope. Databricks Job task parameters can override these defaults.
dbutils.widgets.text(
    "selected_entities",
    "orders,customers,products"
)
dbutils.widgets.text("max_files_per_trigger", "2")

selected_entities_text = dbutils.widgets.get(
    "selected_entities"
).strip()
selected_entities = [
    value.strip()
    for value in selected_entities_text.split(",")
    if value.strip()
]
max_files_per_trigger = int(
    dbutils.widgets.get("max_files_per_trigger")
)


# Explicit schemas keep the expected business columns and datatypes stable.
orders_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("order_timestamp", TimestampType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])

customers_schema = StructType([
    StructField("customer_id", LongType(), True),
    StructField("customer_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("email", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])

products_schema = StructType([
    StructField("product_id", LongType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("_corrupt_record", StringType(), True)
])


# Each entity owns a separate source, checkpoint, and Bronze target.
entity_configs = {
    "orders": {
        "schema": orders_schema,
        "source_path": (
            "abfss://data@demodb117.dfs.core.windows.net/"
            "autoloader/production_session_2/landing/orders/"
        ),
        "checkpoint_path": (
            "abfss://data@demodb117.dfs.core.windows.net/"
            "autoloader/production_session_2/checkpoints/orders/"
        ),
        "target_table": (
            "auto_loader_demo.sales_data.orders_bronze_operations"
        )
    },
    "customers": {
        "schema": customers_schema,
        "source_path": (
            "abfss://data@demodb117.dfs.core.windows.net/"
            "autoloader/production_session_2/landing/customers/"
        ),
        "checkpoint_path": (
            "abfss://data@demodb117.dfs.core.windows.net/"
            "autoloader/production_session_2/checkpoints/customers/"
        ),
        "target_table": (
            "auto_loader_demo.sales_data.customers_bronze_operations"
        )
    },
    "products": {
        "schema": products_schema,
        "source_path": (
            "abfss://data@demodb117.dfs.core.windows.net/"
            "autoloader/production_session_2/landing/products/"
        ),
        "checkpoint_path": (
            "abfss://data@demodb117.dfs.core.windows.net/"
            "autoloader/production_session_2/checkpoints/products/"
        ),
        "target_table": (
            "auto_loader_demo.sales_data.products_bronze_operations"
        )
    }
}


RUN_AUDIT_TABLE = (
    "auto_loader_demo.sales_data."
    "auto_loader_run_audit_operations"
)
BATCH_AUDIT_TABLE = (
    "auto_loader_demo.sales_data."
    "auto_loader_batch_audit_operations"
)


# Create the audit objects if this is the first job execution.
spark.sql(
    "CREATE SCHEMA IF NOT EXISTS auto_loader_demo.sales_data"
)

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS
    auto_loader_demo.sales_data.auto_loader_run_audit_operations (
        pipeline_run_id STRING,
        entity_name STRING,
        query_name STRING,
        started_at TIMESTAMP,
        ended_at TIMESTAMP,
        status STRING,
        run_outcome STRING,
        files_written BIGINT,
        rows_written BIGINT,
        micro_batches BIGINT,
        duration_seconds DOUBLE,
        error_message STRING
    )
    USING DELTA
    """
)

spark.sql(
    """
    CREATE TABLE IF NOT EXISTS
    auto_loader_demo.sales_data.auto_loader_batch_audit_operations (
        pipeline_run_id STRING,
        entity_name STRING,
        query_name STRING,
        progress_timestamp STRING,
        batch_id BIGINT,
        input_rows BIGINT,
        input_rows_per_second DOUBLE,
        processed_rows_per_second DOUBLE,
        files_outstanding BIGINT,
        bytes_outstanding BIGINT,
        latest_offset_ms BIGINT,
        add_batch_ms BIGINT
    )
    USING DELTA
    """
)


run_audit_schema = StructType([
    StructField("pipeline_run_id", StringType(), False),
    StructField("entity_name", StringType(), False),
    StructField("query_name", StringType(), False),
    StructField("started_at", TimestampType(), False),
    StructField("ended_at", TimestampType(), False),
    StructField("status", StringType(), False),
    StructField("run_outcome", StringType(), False),
    StructField("files_written", LongType(), False),
    StructField("rows_written", LongType(), False),
    StructField("micro_batches", LongType(), False),
    StructField("duration_seconds", DoubleType(), False),
    StructField("error_message", StringType(), True)
])

batch_audit_schema = StructType([
    StructField("pipeline_run_id", StringType(), False),
    StructField("entity_name", StringType(), False),
    StructField("query_name", StringType(), False),
    StructField("progress_timestamp", StringType(), True),
    StructField("batch_id", LongType(), False),
    StructField("input_rows", LongType(), False),
    StructField("input_rows_per_second", DoubleType(), False),
    StructField("processed_rows_per_second", DoubleType(), False),
    StructField("files_outstanding", LongType(), True),
    StructField("bytes_outstanding", LongType(), True),
    StructField("latest_offset_ms", LongType(), True),
    StructField("add_batch_ms", LongType(), True)
])


# Purpose: Check job parameters and entity settings before any stream starts.
# This prevents a spelling or configuration error from starting partial work.
def validate_job_configuration(
    configs,
    entities,
    file_limit
):
    required_keys = {
        "schema",
        "source_path",
        "checkpoint_path",
        "target_table"
    }

    unknown_entities = set(entities) - set(configs)
    if unknown_entities:
        raise ValueError(
            "Unknown entities: "
            + ", ".join(sorted(unknown_entities))
        )

    if not entities:
        raise ValueError("At least one entity must be selected.")

    if file_limit <= 0:
        raise ValueError(
            "max_files_per_trigger must be greater than zero."
        )

    for entity_name, config in configs.items():
        missing_keys = required_keys - set(config)
        if missing_keys:
            raise ValueError(
                f"{entity_name} is missing {sorted(missing_keys)}"
            )

    for key in [
        "source_path",
        "checkpoint_path",
        "target_table"
    ]:
        values = [config[key] for config in configs.values()]
        if len(values) != len(set(values)):
            raise ValueError(
                f"Every entity requires a unique {key}."
            )


# Purpose: Convert a reported metric to an integer while keeping a missing
# metric as None.
def optional_int(value):
    return None if value is None else int(value)


# Purpose: Convert a metric to an integer and use the supplied default when
# Databricks reports the metric as None.
def safe_int(value, default=0):
    return default if value is None else int(value)


# Purpose: Convert a reported rate to a decimal number and use 0.0 when the
# metric is unavailable.
def safe_float(value):
    return 0.0 if value is None else float(value)


# Purpose: Build the Auto Loader DataFrame for one entity with its schema,
# rate limit, rescued-data handling, and standard file metadata.
def build_entity_stream(
    entity_name,
    config,
    file_limit
):
    source_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.maxFilesPerTrigger", file_limit)
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .option("readerCaseSensitive", "true")
        .option("mode", "PERMISSIVE")
        .option(
            "columnNameOfCorruptRecord",
            "_corrupt_record"
        )
        .schema(config["schema"])
        .load(config["source_path"])
    )

    return (
        source_df
        .withColumn("entity_name", lit(entity_name))
        .withColumn(
            "source_file_path",
            col("_metadata.file_path")
        )
        .withColumn(
            "source_file_name",
            col("_metadata.file_name")
        )
        .withColumn(
            "source_file_modified_at",
            col("_metadata.file_modification_time")
        )
        .withColumn("ingested_at", current_timestamp())
    )


# Purpose: Convert stream progress events into rows that can be stored in the
# batch-audit table for throughput and backlog monitoring.
def extract_batch_rows(
    pipeline_run_id,
    entity_name,
    query_name,
    progress_events
):
    batch_rows = []

    for progress in progress_events:
        sources = progress.get("sources", [])
        source = sources[0] if sources else {}
        metrics = source.get("metrics", {}) or {}
        durations = progress.get("durationMs", {}) or {}

        # Row and rate metrics can be reported at source level. Fall back to
        # the top-level progress values when the source value is unavailable.
        input_rows_value = source.get("numInputRows")
        if input_rows_value is None:
            input_rows_value = progress.get("numInputRows")

        input_rate_value = source.get("inputRowsPerSecond")
        if input_rate_value is None:
            input_rate_value = progress.get("inputRowsPerSecond")

        processed_rate_value = source.get(
            "processedRowsPerSecond"
        )
        if processed_rate_value is None:
            processed_rate_value = progress.get(
                "processedRowsPerSecond"
            )

        batch_rows.append({
            "pipeline_run_id": pipeline_run_id,
            "entity_name": entity_name,
            "query_name": query_name,
            "progress_timestamp": progress.get("timestamp"),
            "batch_id": safe_int(
                progress.get("batchId"),
                -1
            ),
            "input_rows": safe_int(
                input_rows_value,
                0
            ),
            "input_rows_per_second": safe_float(
                input_rate_value
            ),
            "processed_rows_per_second": safe_float(
                processed_rate_value
            ),
            "files_outstanding": optional_int(
                metrics.get("numFilesOutstanding")
            ),
            "bytes_outstanding": optional_int(
                metrics.get("numBytesOutstanding")
            ),
            "latest_offset_ms": optional_int(
                durations.get("latestOffset")
            ),
            "add_batch_ms": optional_int(
                durations.get("addBatch")
            )
        })

    return batch_rows


# Purpose: Count rows and source files written within the current entity-run
# time window so the run audit contains simple totals.
def count_written_rows_and_files(
    target_table,
    started_at,
    ended_at
):
    rows_from_run = (
        spark.table(target_table)
        .where(
            (col("ingested_at") >= lit(started_at))
            & (col("ingested_at") <= lit(ended_at))
        )
    )

    rows_written = rows_from_run.count()
    files_written = (
        rows_from_run
        .select("source_file_path")
        .distinct()
        .count()
    )

    return rows_written, files_written


# Purpose: Store one final success, no-data, or failure result for an entity.
def save_run_audit(record):
    (
        spark.createDataFrame([record], run_audit_schema)
        .write
        .mode("append")
        .saveAsTable(RUN_AUDIT_TABLE)
    )


# Purpose: Store available micro-batch progress. A no-data run may have no
# progress rows, so the function writes only when records exist.
def save_batch_audit(records):
    if records:
        (
            spark.createDataFrame(records, batch_audit_schema)
            .write
            .mode("append")
            .saveAsTable(BATCH_AUDIT_TABLE)
        )


# Purpose: Run one entity, wait for AvailableNow to finish, capture its
# progress, and write its audit records even when the entity fails.
def run_entity(
    pipeline_run_id,
    entity_name,
    config,
    file_limit
):
    started_at = datetime.now(timezone.utc)
    query_name = (
        f"autoloader_{entity_name}_"
        f"{pipeline_run_id.replace('-', '_')}"
    )
    query = None
    progress_events = []
    status = "SUCCESS"
    error_message = None

    try:
        entity_df = build_entity_stream(
            entity_name,
            config,
            file_limit
        )

        query = (
            entity_df.writeStream
            .format("delta")
            .outputMode("append")
            .queryName(query_name)
            .option(
                "checkpointLocation",
                config["checkpoint_path"]
            )
            .trigger(availableNow=True)
            .toTable(config["target_table"])
        )

        query.awaitTermination()
        progress_events = list(query.recentProgress)

    except Exception as error:
        status = "FAILED"
        error_message = str(error)[:4000]

        if query is not None:
            progress_events = list(query.recentProgress)

    ended_at = datetime.now(timezone.utc)

    try:
        rows_written, files_written = count_written_rows_and_files(
            config["target_table"],
            started_at,
            ended_at
        )
    except Exception:
        rows_written = 0
        files_written = 0

    save_batch_audit(
        extract_batch_rows(
            pipeline_run_id,
            entity_name,
            query_name,
            progress_events
        )
    )

    run_outcome = (
        "FAILED"
        if status == "FAILED"
        else "NO_DATA"
        if rows_written == 0
        else "PROCESSED"
    )

    run_record = {
        "pipeline_run_id": pipeline_run_id,
        "entity_name": entity_name,
        "query_name": query_name,
        "started_at": started_at,
        "ended_at": ended_at,
        "status": status,
        "run_outcome": run_outcome,
        "files_written": int(files_written),
        "rows_written": int(rows_written),
        "micro_batches": int(len(progress_events)),
        "duration_seconds": float(
            (ended_at - started_at).total_seconds()
        ),
        "error_message": error_message
    }
    save_run_audit(run_record)

    print(
        f"{entity_name}: status={status}, "
        f"outcome={run_outcome}, "
        f"files={files_written}, "
        f"rows={rows_written}, "
        f"batches={len(progress_events)}"
    )

    return run_record


# Purpose: Give all selected entities one pipeline run ID, execute them, and
# fail the task after auditing if any entity failed.
def run_entities(
    configs,
    entities,
    file_limit
):
    pipeline_run_id = str(uuid.uuid4())
    results = []

    for entity_name in entities:
        results.append(
            run_entity(
                pipeline_run_id,
                entity_name,
                configs[entity_name],
                file_limit
            )
        )

    failed_entities = [
        result["entity_name"]
        for result in results
        if result["status"] == "FAILED"
    ]

    print(f"Pipeline run ID: {pipeline_run_id}")

    if failed_entities:
        raise RuntimeError(
            "Failed entities: " + ", ".join(failed_entities)
        )

    return pipeline_run_id


validate_job_configuration(
    entity_configs,
    selected_entities,
    max_files_per_trigger
)

pipeline_run_id = run_entities(
    entity_configs,
    selected_entities,
    max_files_per_trigger
)

print(f"Completed pipeline run: {pipeline_run_id}")
```

### 12.3 What belongs in the job notebook

| Included | Excluded |
| --- | --- |
| Schemas and entity configurations | Optional cleanup commands |
| Job parameters | Sample-file generation |
| Auto Loader and audit functions | Intentional invalid configuration |
| Audit-table creation | Display-only validation cells |
| Final execution call | Scenario-specific variables |

### 12.4 Test the dedicated notebook

Before scheduling it:

1. Create at least one new JSON delivery from the scenario notebook.
2. Run `auto_loader_multi_table_job` manually.
3. Confirm that the task processes the new file.
4. Check the run-audit and batch-audit tables.
5. Confirm that a no-data entity is recorded as `SUCCESS` with `NO_DATA`.

### Operational considerations

- The scenario notebook creates files and explains behaviour; the job notebook only performs ingestion and auditing.
- Editing or reordering cells in the scenario notebook cannot change the scheduled job.
- Job parameters can run all entities normally or a selected entity during targeted recovery.
- The job identity needs the same storage and Unity Catalog permissions used during manual validation.

---

## 13. Create the Serverless Job

### 13.1 Create the job

1. Open **Workflows**.
2. Select **Jobs & Pipelines**.
3. Select **Create job**.
4. Name it `auto-loader-multi-table-operations`.
5. Add `auto_loader_multi_table_job` as the notebook task.
6. Select **Serverless** compute.

### 13.2 Configure starting controls

| Setting | Initial value | Purpose |
| --- | --- | --- |
| Timeout | 30 minutes | Prevent an indefinitely running task |
| Retries | 2 | Retry transient failures |
| Retry interval | 5 minutes | Avoid immediate repeated pressure |
| Maximum concurrent runs | 1 | Prevent overlapping executions |
| Notification | Failure notification | Inform the operating team |

The job trigger and the streaming trigger have different roles:

| Control | Responsibility |
| --- | --- |
| Job schedule or file-arrival trigger | Starts the notebook |
| `.trigger(availableNow=True)` | Processes pending files and stops |

Add these task parameters:

| Parameter | Value |
| --- | --- |
| `selected_entities` | `orders,customers,products` |
| `max_files_per_trigger` | `2` |

The dedicated notebook already contains the final execution call. The Job task only supplies parameters and starts the notebook.

### Operational considerations

- Catching every exception without raising a final error can make a failed pipeline appear successful.
- Retries help with transient failures; they do not repair incorrect paths, permissions, schemas, or code.
- Production jobs should use a production service identity rather than an individual user's identity.
- The team receiving failure notifications must own the response process.

---

## 14. Scenario 6: a second run is requested

### Problem

Another run is requested while the current job run is active.

### Concurrency test

1. Start the job manually.
2. Request another run while the first is active.
3. Inspect the run history.
4. Observe whether the second request is queued or skipped under the queue and concurrency settings.
5. Confirm that only one job run actively owns the entity checkpoints.

### Operational considerations

- Job concurrency prevents overlapping executions.
- With queuing enabled, an excess run can wait; without queuing, it can be skipped.
- Checkpoints preserve stream state, while concurrency settings control job execution.
- These mechanisms complement each other but are not interchangeable.

---

## 15. Final job-ready execution

### Cell 1 — Generate a mixed delivery

```python
write_json_records(
    entity_configs["orders"]["source_path"]
    + f"orders_final_{delivery_batch_tag}_001.json",
    [
        {
            "order_id": 3401,
            "customer_id": 1501,
            "order_timestamp": "2026-08-17T14:01:00Z",
            "amount": 3499.0,
            "status": "CONFIRMED"
        },
        {
            "order_id": 3402,
            "customer_id": 1502,
            "order_timestamp": "2026-08-17T14:02:00Z",
            "amount": 1799.0,
            "status": "SHIPPED"
        }
    ]
)

write_json_records(
    entity_configs["products"]["source_path"]
    + f"products_final_{delivery_batch_tag}_001.json",
    [
        {
            "product_id": 1501,
            "product_name": "Production Keyboard",
            "category": "Accessories",
            "price": 3499.0
        }
    ]
)
```

No customer file is created.

### Step 2 — Execute the dedicated job notebook

1. Open `auto-loader-multi-table-operations` under **Jobs & Pipelines**.
2. Select **Run now**.
3. Open the completed task output.
4. Copy the printed pipeline run ID if a specific run needs to be investigated.

The job executes `auto_loader_multi_table_job`; it does not execute the scenario notebook.

### Step 3 — Validate the latest pipeline run

```sql
WITH latest_run AS (
    SELECT pipeline_run_id
    FROM auto_loader_demo.sales_data.auto_loader_run_audit_operations
    ORDER BY started_at DESC
    LIMIT 1
)
SELECT
    audit.entity_name,
    audit.status,
    audit.run_outcome,
    audit.files_written,
    audit.rows_written,
    audit.micro_batches,
    audit.duration_seconds,
    audit.error_message
FROM auto_loader_demo.sales_data.auto_loader_run_audit_operations AS audit
INNER JOIN latest_run
    ON audit.pipeline_run_id = latest_run.pipeline_run_id
ORDER BY audit.entity_name;
```

Expected outcomes:

| Entity | Outcome |
| --- | --- |
| Orders | `PROCESSED` |
| Customers | `NO_DATA` |
| Products | `PROCESSED` |

The job history, run audit, batch audit, checkpoint state, and Bronze metadata should tell a consistent story.

---

## 16. Recovery checklist

| Situation | First evidence | Safe next action |
| --- | --- | --- |
| Job failed | Task output and run audit | Identify the failed entity and cause |
| One entity failed | Entity error and file state | Correct the cause and rerun that entity |
| Entity has `NO_DATA` | Landing folder and delivery SLA | Decide whether no delivery is expected |
| Backlog grows | Batch metrics and file state | Review arrival rate, throughput, and limits |
| Reported file is missing | Landing, checkpoint, Bronze, audit | Classify its actual processing stage |
| Another run is active | Job run history | Allow queue and concurrency policy to act |
| Historical replay requested | Recovery runbook | Use an isolated replay checkpoint and target |

Never delete a production checkpoint as the first response to an ingestion problem.

## Session takeaway

```text
Generate source deliveries in the notebook
        ↓
Rate controls divide pending work
        ↓
Entity audits record outcomes
        ↓
Batch metrics expose progress and backlog
        ↓
File state supports investigation
        ↓
Serverless Jobs control execution
        ↓
Targeted recovery handles failures
```

A production ingestion job must prove what ran, what succeeded, what failed, what remains pending, and what action the operating team should take.

## References

- [Configure Auto Loader for production workloads](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/production)
- [Monitor and observe Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/observability)
- [Configure Structured Streaming triggers](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/triggers)
