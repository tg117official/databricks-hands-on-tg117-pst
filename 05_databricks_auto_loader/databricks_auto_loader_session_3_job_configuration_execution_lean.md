# Session 2 — Databricks Job Configuration and Execution

This session turns the Auto Loader pipeline into a repeatable Lakeflow Job that uses Serverless compute. The notes include the complete job notebook, job-level parameters, current workspace configuration steps, test deliveries, and validation queries in one place.

## Session index

1. [Understand the execution layers](#1-understand-the-execution-layers)
2. [Create the dedicated job notebook](#2-create-the-dedicated-job-notebook)
3. [Create and configure the Lakeflow Job](#3-create-the-lakeflow-job-with-serverless-compute)
4. [Run job-execution scenarios](#4-job-execution-scenarios)
5. [Run and validate a complete delivery](#5-run-and-validate-a-complete-job-delivery)
6. [Use the job troubleshooting checklist](#6-job-troubleshooting-checklist)

---

## 1. Understand the execution layers

The word **job** appears at different levels. Keeping the levels separate makes monitoring easier.

| Layer | Meaning |
| --- | --- |
| Databricks Job | Saved workflow definition containing the notebook task and its settings. |
| Job run | One execution of that workflow definition. |
| Notebook task | The notebook code executed inside the job run. |
| Streaming query | One call to `writeStream` for an entity. |
| Micro-batch | One portion of pending files planned by Structured Streaming. |
| Spark job | Work Spark creates for an action; one micro-batch can produce more than one Spark job. |
| Stage and task | Smaller units used by Spark to distribute the work. |

With ten one-row files and `maxFilesPerTrigger=2`, an `AvailableNow` query normally needs five data micro-batches. They belong to one streaming query inside one Databricks job run, but each micro-batch may launch one or more Spark jobs.

### Job trigger and streaming trigger

| Control | Purpose | If it is missing or misunderstood |
| --- | --- | --- |
| Job schedule, manual run, or file-arrival trigger | Starts the notebook task. | The notebook does not start automatically. |
| `.trigger(availableNow=True)` | Processes data available when the query starts and then stops. | A scheduled Serverless workload would not have the intended finite execution behaviour. |
| `maxFilesPerTrigger` | Limits files planned for one micro-batch. | A large delivery can create an oversized batch; the setting does not limit the entire run. |
| Checkpoint | Remembers files and committed batches across job runs. | A new or deleted checkpoint can cause replay or loss of the expected stream history. |

### Prepare the scenario notebook

Create a Python notebook named `auto_loader_job_session_tests`. It generates test files and runs validation queries; it will not be attached to the job.

```python
import json
from datetime import datetime, timezone


# These are the same landing folders used by the dedicated job notebook.
landing_paths = {
    "orders": (
        "abfss://data@demodb117.dfs.core.windows.net/"
        "autoloader/production_session_2/landing/orders/"
    ),
    "customers": (
        "abfss://data@demodb117.dfs.core.windows.net/"
        "autoloader/production_session_2/landing/customers/"
    ),
    "products": (
        "abfss://data@demodb117.dfs.core.windows.net/"
        "autoloader/production_session_2/landing/products/"
    )
}


# Purpose: Make sure every landing folder exists before a test file is written.
# Without this step, file creation or the first Auto Loader read can fail when
# an entity folder has never been created.
for landing_path in landing_paths.values():
    dbutils.fs.mkdirs(landing_path)


# Purpose: Write newline-delimited JSON directly to ADLS so the scenarios do
# not require a text editor or a separate upload window.
def write_json_records(file_path, records):
    file_content = "\n".join(
        json.dumps(record, separators=(",", ":"))
        for record in records
    )

    # overwrite=False protects an earlier delivery from accidental replacement.
    dbutils.fs.put(
        file_path,
        file_content + "\n",
        overwrite=False
    )


# A UTC timestamp keeps every generated filename unique across repeated tests.
job_test_tag = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
print(f"Job test tag: {job_test_tag}")
```

The notebook identity needs permission to write the landing folders. The job's **Run as** identity separately needs permission to read those files, write the checkpoints, and modify the Unity Catalog tables.

## 2. Create the dedicated job notebook

A demonstration notebook can contain reset cells, test data, inspection queries, and intentional failures. A scheduled job should execute a separate notebook that contains only stable ingestion and auditing code.

Create a separate notebook containing only the stable ingestion code.

### 2.1 Create the notebook

1. Open **Workspace**.
2. Select the folder where the production notebook should be stored.
3. Select **Create** → **Notebook**.
4. Name the notebook `auto_loader_multi_table_job`.
5. Select **Python** as the default language.
6. Paste the complete code below into one Python cell.
7. Save the notebook.

Using one complete cell prevents changes to demonstration-cell order or numbering from affecting the job.

### 2.2 Complete code for `auto_loader_multi_table_job`

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


# Purpose: Read the two required job-level parameters pushed into this
# notebook task by Lakeflow Jobs. Keeping defaults in the job definition gives
# operations one visible source of truth and avoids hiding a missing parameter.
def read_job_parameters():
    try:
        selected_entities_text = dbutils.widgets.get(
            "selected_entities"
        ).strip()
        file_limit_text = dbutils.widgets.get(
            "max_files_per_trigger"
        ).strip()
    except Exception as error:
        raise ValueError(
            "Required job parameters are missing. Configure "
            "selected_entities and max_files_per_trigger in Job details."
        ) from error

    selected_entities = [
        value.strip()
        for value in selected_entities_text.split(",")
        if value.strip()
    ]

    # Job parameter values arrive as text. A clear conversion error is easier
    # to troubleshoot than passing an invalid value into Auto Loader.
    try:
        max_files_per_trigger = int(file_limit_text)
    except ValueError as error:
        raise ValueError(
            "max_files_per_trigger must be a whole number."
        ) from error

    return selected_entities, max_files_per_trigger


selected_entities, max_files_per_trigger = read_job_parameters()


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


# Create the audit objects if this is the first job execution. IF NOT EXISTS
# keeps later runs safe; without these objects the audit writes would fail.
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
        # cloudFiles selects Auto Loader instead of a normal file stream.
        .format("cloudFiles")
        # The landing files are JSON; a wrong format prevents correct parsing.
        .option("cloudFiles.format", "json")
        # This caps files per micro-batch, not files for the complete run.
        .option("cloudFiles.maxFilesPerTrigger", file_limit)
        # Unexpected columns or type mismatches are retained for investigation.
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        # Case-sensitive matching avoids silently mapping differently cased keys.
        .option("readerCaseSensitive", "true")
        # PERMISSIVE keeps malformed JSON available in _corrupt_record.
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
                # Time spent checking the latest available source work.
                durations.get("latestOffset")
            ),
            "add_batch_ms": optional_int(
                # Time spent executing this micro-batch.
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
            # Append adds only rows processed by new micro-batches.
            .outputMode("append")
            # A readable name helps connect Spark progress to the entity run.
            .queryName(query_name)
            .option(
                "checkpointLocation",
                config["checkpoint_path"]
            )
            # AvailableNow drains currently pending files and then terminates.
            .trigger(availableNow=True)
            .toTable(config["target_table"])
        )

        # Wait before collecting progress and writing the final run audit.
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

# Start the actual pipeline only after configuration validation succeeds.
pipeline_run_id = run_entities(
    entity_configs,
    selected_entities,
    max_files_per_trigger
)

print(f"Completed pipeline run: {pipeline_run_id}")
```

### Function guide

| Function | Why it exists and what fails without it |
| --- | --- |
| `read_job_parameters()` | Reads the defaults or run-time overrides supplied by the job and converts the file limit to an integer. Without it, missing or non-numeric values would fail later with a less useful error. |
| `validate_job_configuration()` | Rejects unknown entities, missing settings, duplicate paths, and invalid limits before a stream starts. Without it, some entities could process before a configuration mistake is discovered. |
| `optional_int()` | Preserves an unavailable metric as `None`. Calling `int(None)` would fail after the data write. |
| `safe_int()` / `safe_float()` | Converts reported metrics and supplies safe defaults. Without null handling, audit extraction can fail even when ingestion succeeded. |
| `build_entity_stream()` | Applies the schema, rate control, data-quality handling, and file metadata consistently for every entity. |
| `extract_batch_rows()` | Converts Spark progress dictionaries into rows that match the batch-audit table. |
| `count_written_rows_and_files()` | Produces simple run totals from the ingestion-time window. Concurrent writers could make this learning-friendly method inaccurate, which is another reason to keep job concurrency at one. |
| `save_run_audit()` / `save_batch_audit()` | Persist evidence after notebook output is no longer open. Without these writes, historical troubleshooting depends only on temporary task logs. |
| `run_entity()` | Starts one entity query, waits for it, captures progress, and audits success or failure. |
| `run_entities()` | Gives selected entities one pipeline-run ID and raises a final error when any entity failed. Without the final raise, the Databricks task could show success after an entity failure. |

### 2.3 Keep the job notebook focused

| Included | Excluded |
| --- | --- |
| Schemas and entity configurations | Optional cleanup commands |
| Reading and validating job parameters | Sample-file generation |
| Auto Loader and audit functions | Intentional invalid configuration |
| Audit-table creation | Display-only validation cells |
| Final execution call | Scenario-specific variables |

### 2.4 Prepare a test delivery

Before creating the job:

1. Create at least one new JSON delivery from the scenario notebook.
2. Do not run `auto_loader_multi_table_job` as a standalone notebook yet.
3. Create the job and its parameters in the next section.
4. Use **Run now** to perform the first complete test.

The notebook deliberately does not create parameter defaults. A standalone run therefore reports that the required parameters are missing. This protects the job from silently using notebook values that differ from its visible configuration.

### Operational considerations

- The scenario notebook creates test files and runs checks; the job notebook only performs ingestion and auditing.
- Editing or reordering cells in the scenario notebook cannot change the scheduled job.
- Job parameters can run all entities normally or a selected entity during targeted recovery.
- The job identity needs storage and Unity Catalog permissions even if the user creating the test file has broader access.

---

## 3. Create the Lakeflow Job with Serverless compute

The settings are split between the task panel and the Job details pane:

| Configuration | Current UI location |
| --- | --- |
| Notebook, Serverless compute, retries | Select the notebook task |
| Timeout | Notebook task → **Metric thresholds** → **Run duration** |
| Default parameters | **Job details** → **Edit parameters** |
| Maximum concurrent runs and queueing | **Job details** → **Advanced settings** |
| Notifications and Run as | **Job details** |

Selecting the task shows task settings; returning to **Job details** shows controls that apply to the complete job.

### 3.1 Create the job

1. In the left sidebar, select **Jobs & Pipelines**.
2. Select **Create** → **Job**.
3. Select the **Notebook** task type.
4. Enter the task name `run_auto_loader_multi_table`.
5. In **Path**, select `auto_loader_multi_table_job`.
6. Use **Serverless** compute.
7. Select **Save task**.
8. Rename the job to `auto-loader-multi-table-operations`.

The job is the saved orchestration definition; the notebook is one task inside it. Saving the notebook alone does not create a runnable job.

### 3.2 Define default parameters at the job level

1. Open the job's **Job details** pane.
2. Select **Edit parameters**.
3. Add the following key-value pairs.

| Key | Default value |
| --- | --- |
| `selected_entities` | `orders,customers,products` |
| `max_files_per_trigger` | `2` |

4. Select **Save**.

These are job-level defaults. Notebook tasks accept key-value parameters, so Lakeflow Jobs pushes both values into the notebook automatically. The code reads them using:

```python
dbutils.widgets.get("selected_entities")
dbutils.widgets.get("max_files_per_trigger")
```

Do not add duplicate parameters to the notebook task. If the same key exists at both job and task level, the job parameter takes precedence, which makes the extra task value misleading.

Do not recreate these defaults with `dbutils.widgets.text()` in the production notebook. Notebook defaults are useful for standalone testing, but here they could hide a missing job configuration.

| Parameter | Purpose | Example failure |
| --- | --- | --- |
| `selected_entities` | Runs all entities or a smaller recovery scope. | `orders,unknown` fails validation before a stream starts. |
| `max_files_per_trigger` | Limits the files planned for one micro-batch. | `0` is rejected; a very small positive number can make a large backlog take longer to drain. |

### 3.3 Configure task timeout and retries

Select the notebook task in the task graph, then use the task configuration panel.

#### Configure the timeout under Metric thresholds

1. Open **Metric thresholds**.
2. Add a threshold.
3. In **Metric**, select **Run duration**.
4. Leave **Warning** empty for this exercise, or add a warning only if slow-run notifications are required.
5. Enter `30 minutes` in **Timeout**.
6. Save the task.

The **Warning** value raises an event when a task is slower than expected. The **Timeout** value stops the task and marks it `Timed Out` when the maximum duration is exceeded. For this session, timeout is configured at the task level under **Metric thresholds**; no job-level run-duration setting is required.

If the workspace does not show **Run duration** in **Job details**, continue with the task-level setting above. A 30-minute limit is a starting value: an unrealistically small timeout can stop a healthy backlog, while no timeout can leave a stuck task active.

#### Configure retries

1. In the same task configuration panel, locate **Retries**.
2. Select **Add**.
3. Set **Retries** to `2`.
4. Set the retry interval to `5 minutes`.
5. Save the task.

The timeout applies separately to each retry attempt. Retries can help with temporary platform, storage, or network issues; they only repeat a permanent error such as an invalid parameter, path, permission, or code defect.

### 3.4 Confirm job-level concurrency and queueing

1. Return to the **Job details** pane.
2. Expand **Advanced settings**.
3. Under **Concurrent runs**, select the edit control when it is available.
4. Confirm **Maximum concurrent runs = 1**.
5. Confirm **Queue** is enabled.

New jobs default to one maximum concurrent run. If the edit control is not visible, no change is required as long as the job remains at the default. Keeping the value at one prevents overlapping runs from trying to operate on the same entity checkpoints.

Queueing lets a second request wait until the current run finishes. Without queueing, a run requested after the concurrency limit is reached can be skipped.

### 3.5 Configure the remaining operational controls

| Setting | Starting value | Purpose and risk when missing or incorrect |
| --- | --- | --- |
| Compute | Serverless | Runs the notebook without managing a cluster. The notebook uses `AvailableNow`, which is supported and recommended for finite Serverless streaming work. |
| Failure notification | Team destination | Reports failed or timed-out runs. A notification without a clear owner still leaves the incident unattended. |
| Run as | Approved service principal when available | Gives the job a stable non-person identity. Using an individual account can break the job when that user leaves or loses access. |

The job trigger and the streaming trigger have different roles:

| Control | Responsibility |
| --- | --- |
| Job schedule or file-arrival trigger | Starts the notebook |
| `.trigger(availableNow=True)` | Processes pending files and stops |

### 3.6 Understand execution identity

Job permissions and **Run as** permissions answer different questions:

- Job permissions control who can view, run, or edit the job definition.
- The **Run as** identity supplies the permissions used while the task accesses ADLS, checkpoints, and Unity Catalog.

Grant the Run as identity only the access required for these source paths, checkpoint paths, and tables. A user may be allowed to click **Run now** while the task still fails because the Run as identity cannot read or write the data.

### Operational considerations

- Catching every exception without raising a final error can make a failed pipeline appear successful.
- Retries help with transient failures; they do not repair incorrect paths, permissions, schemas, or code.
- Production jobs should use a production service identity rather than an individual user's identity.
- The team receiving failure notifications must own the response process.

---

## 4. Job execution scenarios

### Scenario 1 — One job run contains several micro-batches

Create five order files in `auto_loader_job_session_tests`:

```python
for file_number in range(1, 6):
    write_json_records(
        landing_paths["orders"]
        + f"orders_job_burst_{job_test_tag}_{file_number:03d}.json",
        [
            {
                "order_id": 4000 + file_number,
                "customer_id": 2000 + file_number,
                "order_timestamp": (
                    f"2026-08-18T09:{file_number:02d}:00Z"
                ),
                "amount": float(700 + file_number * 100),
                "status": "CONFIRMED"
            }
        ]
    )
```

Open the caret beside **Run now**, select **Run now with different settings**, and override:

| Parameter | Value |
| --- | --- |
| `selected_entities` | `orders` |
| `max_files_per_trigger` | `2` |

Inspect the latest orders run:

```sql
WITH latest_orders_run AS (
    SELECT pipeline_run_id
    FROM auto_loader_demo.sales_data.auto_loader_run_audit_operations
    WHERE entity_name = 'orders'
    ORDER BY started_at DESC
    LIMIT 1
)
SELECT
    batch_id,
    input_rows,
    files_outstanding,
    latest_offset_ms,
    add_batch_ms
FROM auto_loader_demo.sales_data.auto_loader_batch_audit_operations
WHERE pipeline_run_id = (
    SELECT pipeline_run_id FROM latest_orders_run
)
ORDER BY batch_id;
```

Five one-row files with a limit of two normally produce three data micro-batches: `2`, `2`, and `1`. Runtime progress can include additional events, so validate the total rows and backlog as well as the event count.

| Metric | Short interpretation |
| --- | --- |
| `files_outstanding` | Files still waiting; a falling value means the query is draining the backlog. |
| `latest_offset_ms` | Time spent checking for the latest available source work. |
| `add_batch_ms` | Time spent reading, processing, and writing the micro-batch. |

### Scenario 2 — A successful run has no new data

Run the job again with the same parameters without creating another file. The checkpoint remembers the five files, so the expected entity result is:

```text
status      = SUCCESS
run_outcome = NO_DATA
files       = 0
rows        = 0
```

`NO_DATA` is not a technical failure. It becomes an operational issue only when a delivery was expected.

### Scenario 3 — Run only one selected entity

Create one customer delivery:

```python
write_json_records(
    landing_paths["customers"]
    + f"customers_job_recovery_{job_test_tag}_001.json",
    [
        {
            "customer_id": 2101,
            "customer_name": "Job Recovery Customer",
            "city": "Pune",
            "email": "job.recovery@example.com"
        }
    ]
)
```

Open the caret beside **Run now**, select **Run now with different settings**, and override:

| Parameter | Value |
| --- | --- |
| `selected_entities` | `customers` |
| `max_files_per_trigger` | `2` |

Only the customers query starts. This is useful after one entity fails; running every healthy entity again adds noise without improving recovery.

### Scenario 4 — Invalid parameters and retries

Run with `selected_entities=orders,unknown`. `validate_job_configuration()` rejects `unknown` before any streaming query starts, and the task must show **Failed**.

If two retries are configured, Databricks can repeat the task, but every attempt will fail until the parameter is corrected. Retries help with temporary platform or network problems; they do not repair deterministic configuration errors.

An invocation rejected before `run_entities()` may not have a `pipeline_run_id` or run-audit row. Use the task output for this early validation failure.

### Scenario 5 — A second run is requested

1. Start a run while several files are pending.
2. Request another run before the first one finishes.
3. Inspect the job-run history.
4. Confirm that maximum concurrent runs remains `1`.
5. With queueing enabled, confirm that the second request waits and starts later.

Concurrency prevents two runs from actively using the same entity checkpoints. Queueing decides whether an additional request waits or is skipped; it does not replace checkpointing.

---

## 5. Run and validate a complete job delivery

### Step 1 — Generate a mixed delivery

```python
write_json_records(
    landing_paths["orders"]
    + f"orders_final_{job_test_tag}_001.json",
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
    landing_paths["products"]
    + f"products_final_{job_test_tag}_001.json",
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

### Step 4 — Cross-check the checkpoint and Bronze table

```sql
SELECT
    path,
    ingestion_state,
    discovery_time,
    processed_time,
    commit_time
FROM cloud_files_state(
    'abfss://data@demodb117.dfs.core.windows.net/autoloader/production_session_2/checkpoints/orders/'
)
WHERE path LIKE '%orders_final%'
ORDER BY discovery_time DESC;
```

A completed file has `ingestion_state = 'INGESTED'` and a non-null `commit_time`. This state comes from the orders checkpoint; it is not calculated from the Delta table.

```sql
SELECT
    order_id,
    source_file_name,
    ingested_at
FROM auto_loader_demo.sales_data.orders_bronze_operations
WHERE source_file_name LIKE 'orders_final%'
ORDER BY ingested_at DESC;
```

The checkpoint proves the file reached a committed stream state, while Bronze metadata proves which records were written from that file.

### Identifiers used during investigation

| Identifier | Scope |
| --- | --- |
| Job ID | Saved Databricks Job definition. |
| Job run ID | One orchestration attempt visible in job-run history. |
| Task run and retry attempt | One execution or retry of the notebook task. |
| `pipeline_run_id` | Custom ID shared by entity audit rows created by one call to `run_entities()`. |
| `query_name` | One entity's Structured Streaming query inside that pipeline run. |
| `batch_id` | Micro-batch number maintained by that query's checkpoint. |

These IDs are related but not interchangeable. Start with the failed job run, open its task output, copy the printed `pipeline_run_id`, and then use that value in the audit tables.

---

## 6. Job troubleshooting checklist

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
- [Configure Structured Streaming triggers](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/triggers)
- [Configure and edit Lakeflow Jobs](https://learn.microsoft.com/en-us/azure/databricks/jobs/configure-job)
- [Configure notebook tasks, retries, and Metric thresholds](https://learn.microsoft.com/en-us/azure/databricks/jobs/configure-task)
- [Configure job parameters](https://learn.microsoft.com/en-us/azure/databricks/jobs/job-parameters)
- [Access parameters from a notebook task](https://learn.microsoft.com/en-us/azure/databricks/jobs/parameter-use)
- [Run a job with default or overridden parameters](https://learn.microsoft.com/en-us/azure/databricks/jobs/run-now)
- [Manage job identities and privileges](https://learn.microsoft.com/en-us/azure/databricks/jobs/privileges)
- [Use cloud_files_state](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/cloud_files_state)
