"""
Reusable helpers for demonstrating customer schema evolution in the PySpark shell.

Load this file after starting the shell:

    exec(open("customer_schema_evolution_shell_helpers.py", encoding="utf-8").read())

Nothing runs automatically. Each command can be executed separately so that the
streaming behaviour and output can be observed step by step.
"""

from pathlib import Path
import shutil
from typing import Any, Callable

from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


# -----------------------------------------------------------------------------
# Project folders
# -----------------------------------------------------------------------------

# Start the PySpark shell from the folder containing this helper file.
# Path.cwd() then becomes the project directory.
PROJECT_DIR = Path.cwd()
RUNTIME_DIR = PROJECT_DIR / "schema_evolution_runtime"
SAMPLE_DIR = PROJECT_DIR / "sample_files"
LANDING_ROOT = RUNTIME_DIR / "landing" / "customers"
RAW_ROOT = RUNTIME_DIR / "raw_zone" / "customers"
CHECKPOINT_ROOT = RUNTIME_DIR / "checkpoints" / "customers"


# -----------------------------------------------------------------------------
# Customer schema versions
# -----------------------------------------------------------------------------

# Schema V1 is the original customer data contract.
#
# Expected columns:
# customer_id, customer_name, city, email
CUSTOMER_SCHEMA_V1 = StructType(
    [
        StructField("customer_id", IntegerType(), True),
        StructField("customer_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("email", StringType(), True),
    ]
)


# Schema V2 adds phone_number at the end.
#
# Adding a nullable column at the end is usually easier to support because an
# old file can still be read and phone_number can be stored as null.
CUSTOMER_SCHEMA_V2 = StructType(
    [
        StructField("customer_id", IntegerType(), True),
        StructField("customer_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
    ]
)


# Schema V3 changes customer_id from integer to string.
#
# This supports values such as C110. It is a breaking change because downstream
# systems may already expect customer_id to be numeric.
CUSTOMER_SCHEMA_V3 = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
    ]
)


# -----------------------------------------------------------------------------
# Scenario configuration
# -----------------------------------------------------------------------------

# enforce_schema=True applies the supplied schema by CSV column position.
# This can allow reordered string columns to enter the wrong fields.
#
# enforce_schema=False asks Spark to validate the CSV header against the
# supplied schema. Header drift is then more likely to fail visibly.
SCENARIOS: dict[str, dict[str, Any]] = {
    "baseline": {
        "title": "Baseline file matches schema V1",
        "file_name": "customers_01_baseline.csv",
        "schema": CUSTOMER_SCHEMA_V1,
        "schema_version": "v1",
        "enforce_schema": True,
        "expected": (
            "All four customer columns should be stored correctly. This is the "
            "reference result before introducing schema changes."
        ),
    },
    "added_column_old_schema": {
        "title": "Source adds phone_number but the pipeline still uses V1",
        "file_name": "customers_02_added_phone_old_schema.csv",
        "schema": CUSTOMER_SCHEMA_V1,
        "schema_version": "v1",
        "enforce_schema": True,
        "expected": (
            "The pipeline still exposes only four columns. phone_number is not "
            "captured, demonstrating that source evolution does not "
            "automatically evolve an explicit Spark schema."
        ),
    },
    "old_file_new_schema": {
        "title": "An old V1 file arrives after the pipeline upgrades to V2",
        "file_name": "customers_03_old_file_new_schema.csv",
        "schema": CUSTOMER_SCHEMA_V2,
        "schema_version": "v2",
        "enforce_schema": True,
        "expected": (
            "phone_number should be null because the old file does not contain "
            "the newly added trailing field."
        ),
    },
    "reordered_columns_unsafe": {
        "title": "email and city are reordered while positional mapping is used",
        "file_name": "customers_04_reordered_columns.csv",
        "schema": CUSTOMER_SCHEMA_V2,
        "schema_version": "v2",
        "enforce_schema": True,
        "expected": (
            "The query can succeed while email is stored in city and city is "
            "stored in email. This demonstrates silent data corruption."
        ),
    },
    "reordered_columns_safe": {
        "title": "Reordered columns are checked using header validation",
        "file_name": "customers_04_reordered_columns.csv",
        "schema": CUSTOMER_SCHEMA_V2,
        "schema_version": "v2",
        "enforce_schema": False,
        "expected": (
            "The micro-batch should fail because the header order does not match "
            "the expected customer data contract."
        ),
    },
    "renamed_columns": {
        "title": "customer_name and email are renamed by the source",
        "file_name": "customers_05_renamed_columns.csv",
        "schema": CUSTOMER_SCHEMA_V2,
        "schema_version": "v2",
        "enforce_schema": False,
        "expected": (
            "The file should be rejected because full_name and email_address do "
            "not match the expected header. A rename is a breaking change."
        ),
    },
    "datatype_change_old_schema": {
        "title": "customer_id becomes alphanumeric but schema V2 expects integer",
        "file_name": "customers_06_alphanumeric_id.csv",
        "schema": CUSTOMER_SCHEMA_V2,
        "schema_version": "v2",
        "enforce_schema": True,
        "expected": (
            "Under permissive parsing, C110 can become null because it cannot be "
            "parsed as an integer. The query may succeed while losing the "
            "business key."
        ),
    },
    "datatype_change_new_schema": {
        "title": "Schema V3 accepts customer_id as a string",
        "file_name": "customers_06_alphanumeric_id.csv",
        "schema": CUSTOMER_SCHEMA_V3,
        "schema_version": "v3",
        "enforce_schema": True,
        "expected": (
            "C110 should be stored successfully as a string. This demonstrates a "
            "controlled pipeline upgrade for a breaking datatype change."
        ),
    },
}


# Active queries are stored here so that they can be inspected and stopped by
# scenario name from the shell.
ACTIVE_QUERIES: dict[str, StreamingQuery] = {}


# -----------------------------------------------------------------------------
# Convenience functions
# -----------------------------------------------------------------------------

def list_scenarios() -> None:
    """Display the available scenario names in their recommended order."""
    print("Available scenarios:\n")
    for number, (name, config) in enumerate(SCENARIOS.items(), start=1):
        print(f"{number}. {name}")
        print(f"   {config['title']}")


def explain_scenario(scenario_name: str) -> None:
    """Display the scenario purpose and expected behaviour."""
    config = _get_scenario(scenario_name)
    print(f"\n{config['title']}")
    print("-" * len(config["title"]))
    print(config["expected"])
    print(f"\nSchema version: {config['schema_version']}")
    print(f"enforceSchema: {str(config['enforce_schema']).lower()}")
    print(f"Sample file: {SAMPLE_DIR / config['file_name']}")


def show_input_file(scenario_name: str) -> None:
    """Print the exact CSV content used by a scenario."""
    config = _get_scenario(scenario_name)
    sample_file = SAMPLE_DIR / config["file_name"]
    print(f"\n{sample_file}\n")
    print(sample_file.read_text(encoding="utf-8"))


def prepare_scenario(scenario_name: str) -> dict[str, Path]:
    """
    Create clean landing, raw, and checkpoint folders for one scenario.

    Run this before starting a query. Cleaning the checkpoint lets the same
    classroom scenario be demonstrated repeatedly from batch 0.
    """
    _get_scenario(scenario_name)
    stop_customer_stream(scenario_name, quiet=True)

    paths = scenario_paths(scenario_name)
    for path in paths.values():
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(parents=True, exist_ok=True)

    print(f"Prepared scenario: {scenario_name}")
    print(f"Landing    : {paths['landing']}")
    print(f"Raw output : {paths['raw']}")
    print(f"Checkpoint : {paths['checkpoint']}")
    return paths


def start_customer_stream(scenario_name: str) -> StreamingQuery:
    """
    Start one customer streaming query and return it to the shell.

    The query remains active until stop_customer_stream() or query.stop() is
    called. Start the query before copying the sample file into the landing
    directory so that file arrival can be demonstrated clearly.
    """
    config = _get_scenario(scenario_name)
    paths = scenario_paths(scenario_name)

    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)

    if scenario_name in ACTIVE_QUERIES and ACTIVE_QUERIES[scenario_name].isActive:
        raise RuntimeError(
            f"The query for '{scenario_name}' is already active. Stop it first."
        )

    customer_stream = (
        spark.readStream
        .format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("enforceSchema", str(config["enforce_schema"]).lower())
        .schema(config["schema"])
        .load(str(paths["landing"]))
    )

    enriched_stream = (
        customer_stream
        .withColumn("source_file", input_file_name())
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("schema_version", lit(config["schema_version"]))
        .withColumn("scenario_name", lit(scenario_name))
    )

    batch_writer = _create_batch_writer(paths["raw"])

    query = (
        enriched_stream.writeStream
        .queryName(f"customers_{scenario_name}")
        .outputMode("append")
        .foreachBatch(batch_writer)
        .option("checkpointLocation", str(paths["checkpoint"]))
        .trigger(processingTime="5 seconds")
        .start()
    )

    ACTIVE_QUERIES[scenario_name] = query

    print(f"Started query: {query.name}")
    print(f"Copy the sample file by running: arrive_file('{scenario_name}')")
    return query


def arrive_file(scenario_name: str) -> Path:
    """
    Copy the complete sample file into the monitored landing directory.

    The sample remains available for another demonstration. The landing copy is
    treated as the newly arrived source file.
    """
    config = _get_scenario(scenario_name)
    paths = scenario_paths(scenario_name)
    paths["landing"].mkdir(parents=True, exist_ok=True)

    source_file = SAMPLE_DIR / config["file_name"]
    destination_file = paths["landing"] / config["file_name"]

    if not source_file.exists():
        raise FileNotFoundError(f"Sample file not found: {source_file}")

    if destination_file.exists():
        destination_file.unlink()

    shutil.copy2(source_file, destination_file)
    print(f"File arrived: {destination_file}")
    return destination_file


def process_available_data(scenario_name: str) -> None:
    """
    Ask Spark to finish processing all files currently available.

    For a valid scenario, the command returns after the current file is written.
    For a deliberate breaking-change scenario, Spark may raise the parsing or
    header-validation error here.
    """
    query = get_query(scenario_name)
    query.processAllAvailable()
    print(f"All currently available data was processed for: {scenario_name}")


def show_raw_output(scenario_name: str) -> DataFrame | None:
    """
    Read all Parquet files created for the scenario and display the result.

    recursiveFileLookup is used because every micro-batch is stored in a
    separate batch_<id> directory.
    """
    paths = scenario_paths(scenario_name)
    parquet_files = list(paths["raw"].rglob("*.parquet"))

    if not parquet_files:
        print(f"No Parquet output exists for: {scenario_name}")
        return None

    result_df = (
        spark.read
        .option("recursiveFileLookup", "true")
        .parquet(str(paths["raw"]))
    )

    result_df.printSchema()
    result_df.show(truncate=False)
    return result_df


def show_query_status(scenario_name: str) -> None:
    """Display the current state and latest progress of a query."""
    query = get_query(scenario_name)
    print(f"Query name : {query.name}")
    print(f"Is active  : {query.isActive}")
    print(f"Status     : {query.status}")
    print(f"Exception  : {query.exception()}")
    print(f"Last batch : {query.lastProgress}")


def stop_customer_stream(scenario_name: str, quiet: bool = False) -> None:
    """Stop the selected customer query if it exists and is active."""
    query = ACTIVE_QUERIES.get(scenario_name)

    if query is None:
        if not quiet:
            print(f"No query has been started for: {scenario_name}")
        return

    if query.isActive:
        query.stop()
        if not quiet:
            print(f"Stopped query: {query.name}")
    elif not quiet:
        print(f"Query is already stopped: {query.name}")


def stop_all_customer_streams() -> None:
    """Stop every query started through this helper file."""
    for scenario_name in list(ACTIVE_QUERIES):
        stop_customer_stream(scenario_name)


def scenario_paths(scenario_name: str) -> dict[str, Path]:
    """Return the landing, raw, and checkpoint paths for a scenario."""
    _get_scenario(scenario_name)
    return {
        "landing": LANDING_ROOT / scenario_name / "incoming",
        "raw": RAW_ROOT / scenario_name / "batches",
        "checkpoint": CHECKPOINT_ROOT / scenario_name,
    }


def get_query(scenario_name: str) -> StreamingQuery:
    """Return the active or completed query object for a scenario."""
    _get_scenario(scenario_name)
    query = ACTIVE_QUERIES.get(scenario_name)
    if query is None:
        raise RuntimeError(
            f"No query exists for '{scenario_name}'. "
            f"Run start_customer_stream('{scenario_name}') first."
        )
    return query


# -----------------------------------------------------------------------------
# Internal helpers
# -----------------------------------------------------------------------------

def _get_scenario(scenario_name: str) -> dict[str, Any]:
    if scenario_name not in SCENARIOS:
        valid = ", ".join(SCENARIOS)
        raise ValueError(
            f"Unknown scenario '{scenario_name}'. Choose one of: {valid}"
        )
    return SCENARIOS[scenario_name]


def _create_batch_writer(raw_path: Path) -> Callable[[DataFrame, int], None]:
    """Create a foreachBatch writer that uses one output folder per batch."""

    def write_one_batch(batch_df: DataFrame, batch_id: int) -> None:
        batch_output_path = raw_path / f"batch_{batch_id:020d}"

        output_df = (
            batch_df
            .withColumn("streaming_batch_id", lit(batch_id))
            .withColumn("batch_written_at", current_timestamp())
        )

        # overwrite applies only to this unique batch folder. If Spark retries
        # the same batch ID before committing checkpoint progress, the same
        # folder is replaced instead of creating a second copy of that batch.
        (
            output_df.write
            .format("parquet")
            .mode("overwrite")
            .save(str(batch_output_path))
        )

        print(f"Batch {batch_id} written to: {batch_output_path}")

    return write_one_batch


print("Customer schema-evolution helpers loaded.")
print("Run list_scenarios() to see the available demonstrations.")
