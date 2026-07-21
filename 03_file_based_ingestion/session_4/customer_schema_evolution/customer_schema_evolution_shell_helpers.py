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
        "problem": (
            "The incoming file follows schema V2 and contains phone_number, but "
            "the streaming query still applies schema V1. The new field is not "
            "present in the DataFrame or raw output."
        ),
        "spark_behavior": (
            "Spark builds the DataFrame from the explicitly declared V1 fields. "
            "The query can complete successfully even though phone_number is "
            "not captured."
        ),
        "impact": (
            "The pipeline appears healthy, but useful source data is silently "
            "lost."
        ),
        "solutions": [
            "Upgrade the explicit schema to V2 and add phone_number as a nullable field.",
            "Validate the incoming header before ingestion and alert when unapproved extra columns appear.",
            "Run V1 and V2 readers separately during a controlled source cutover.",
            "Preserve the original source file unchanged so it can be reparsed if the pipeline schema was outdated.",
        ],
        "recommended": (
            "For an approved source change, introduce schema V2, keep the new "
            "field nullable during the transition, test both V1 and V2 files, "
            "and use a versioned checkpoint and output location."
        ),
    },
    "old_file_new_schema": {
        "title": "An old V1 file arrives after the pipeline upgrades to V2",
        "file_name": "customers_03_old_file_new_schema.csv",
        "schema": CUSTOMER_SCHEMA_V2,
        "schema_version": "v2",
        "enforce_schema": True,
        "problem": (
            "The pipeline expects phone_number, but an older producer still "
            "sends the four-column V1 file."
        ),
        "spark_behavior": (
            "The existing fields are read and the missing trailing phone_number "
            "field is stored as null."
        ),
        "impact": (
            "This is acceptable only when phone_number is optional. If it is a "
            "required business field, the record is incomplete."
        ),
        "solutions": [
            "Keep phone_number nullable for a limited transition period.",
            "Apply a default only when the default has a valid business meaning.",
            "Route old and new file versions through separate schema-specific readers.",
            "Reject old-format files after an agreed cutover date.",
            "Add a data-quality check and monitor the percentage of missing phone numbers.",
        ],
        "recommended": (
            "Allow null during a time-bound migration, monitor old-format file "
            "usage, and enforce V2 after every producer has completed the cutover."
        ),
    },
    "reordered_columns_unsafe": {
        "title": "email and city are reordered while positional mapping is used",
        "file_name": "customers_04_reordered_columns.csv",
        "schema": CUSTOMER_SCHEMA_V2,
        "schema_version": "v2",
        "enforce_schema": True,
        "problem": (
            "The source changes the order of email and city, while the pipeline "
            "continues applying schema V2 by column position."
        ),
        "spark_behavior": (
            "Because both fields are strings, Spark can store email in city and "
            "city in email without producing a datatype error."
        ),
        "impact": (
            "The query succeeds but the raw data is semantically wrong. This is "
            "silent data corruption."
        ),
        "solutions": [
            "Enable header validation with enforceSchema set to false.",
            "Validate file headers before moving files into the monitored folder.",
            "Create an explicit mapping only for a known and approved schema version.",
            "Use a self-describing format such as Parquet, Avro, or JSON when the source can support it.",
        ],
        "recommended": (
            "Do not accept unexpected reordering. Reject the file, alert the "
            "source owner, and add an explicit versioned mapping only when the "
            "change is approved."
        ),
    },
    "reordered_columns_safe": {
        "title": "Reordered columns are checked using header validation",
        "file_name": "customers_04_reordered_columns.csv",
        "schema": CUSTOMER_SCHEMA_V2,
        "schema_version": "v2",
        "enforce_schema": False,
        "problem": (
            "The incoming header order does not match the agreed V2 contract."
        ),
        "spark_behavior": (
            "Header validation detects the mismatch and the micro-batch fails "
            "before incorrect rows are written."
        ),
        "impact": (
            "Ingestion pauses for this file, but trusted data is protected from "
            "silent corruption."
        ),
        "solutions": [
            "Correct the file at the source and resend it in the agreed order.",
            "Quarantine the file and notify the source owner.",
            "Add a controlled mapping for the new order only when it becomes an approved contract version.",
            "Move to a self-describing file format if column reordering is expected frequently.",
        ],
        "recommended": (
            "Keep header validation enabled. Reject or quarantine the unexpected "
            "file instead of disabling the safeguard."
        ),
    },
    "renamed_columns": {
        "title": "customer_name and email are renamed by the source",
        "file_name": "customers_05_renamed_columns.csv",
        "schema": CUSTOMER_SCHEMA_V2,
        "schema_version": "v2",
        "enforce_schema": False,
        "problem": (
            "The source sends full_name and email_address, while the contract "
            "still defines customer_name and email."
        ),
        "spark_behavior": (
            "Header validation rejects the file because the supplied names no "
            "longer match schema V2."
        ),
        "impact": (
            "The ingestion contract and any downstream code using the old names "
            "are affected."
        ),
        "solutions": [
            "Ask the producer to restore the agreed column names.",
            "Explicitly map full_name to customer_name and email_address to email during a planned transition.",
            "Run separate V1 and V2 contract versions while consumers migrate.",
            "Publish a new schema version and update downstream queries, reports, and APIs.",
        ],
        "recommended": (
            "Treat a rename as a breaking change. Use an explicit approved "
            "mapping or a new contract version; never rely on positional mapping "
            "to hide the rename."
        ),
    },
    "datatype_change_old_schema": {
        "title": "customer_id becomes alphanumeric but schema V2 expects integer",
        "file_name": "customers_06_alphanumeric_id.csv",
        "schema": CUSTOMER_SCHEMA_V2,
        "schema_version": "v2",
        "enforce_schema": True,
        "problem": (
            "The source starts sending customer IDs such as C110, but schema V2 "
            "still defines customer_id as IntegerType."
        ),
        "spark_behavior": (
            "Under permissive CSV parsing, the unparseable identifier can become "
            "null while the query continues."
        ),
        "impact": (
            "The customer business key is lost, which can break joins, updates, "
            "deduplication, and reconciliation."
        ),
        "solutions": [
            "Reject records or files when the business key cannot be parsed.",
            "Read identifiers as strings and validate their allowed pattern separately.",
            "Use a strict parsing mode or a data-quality rule for mandatory keys.",
            "Create a new schema version and migrate downstream consumers.",
        ],
        "recommended": (
            "Store identifiers as strings when alphanumeric values are possible, "
            "make the key mandatory, validate its format, and deploy the change "
            "as schema V3."
        ),
    },
    "datatype_change_new_schema": {
        "title": "Schema V3 accepts customer_id as a string",
        "file_name": "customers_06_alphanumeric_id.csv",
        "schema": CUSTOMER_SCHEMA_V3,
        "schema_version": "v3",
        "enforce_schema": True,
        "problem": (
            "Schema V3 can ingest C110, but older tables, joins, APIs, or reports "
            "may still expect an integer customer_id."
        ),
        "spark_behavior": (
            "Spark now preserves C110 correctly as a string. Numeric historical "
            "IDs are also representable as strings."
        ),
        "impact": (
            "The ingestion problem is solved, but downstream compatibility still "
            "has to be managed."
        ),
        "solutions": [
            "Adopt string as the canonical datatype for customer_id across the platform.",
            "Temporarily provide a compatibility column or view for consumers that still require numeric IDs.",
            "Migrate downstream schemas and joins in a controlled sequence.",
            "Test both historic numeric IDs and new alphanumeric IDs before deployment.",
        ],
        "recommended": (
            "Use string as the canonical customer identifier, version the change, "
            "test old and new values, and update downstream consumers before the "
            "old numeric contract is retired."
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
    """Display the problem, observed behaviour, and response options."""
    config = _get_scenario(scenario_name)
    print(f"\n{config['title']}")
    print("-" * len(config["title"]))

    if scenario_name == "baseline":
        print(config["expected"])
    else:
        print("\nProblem")
        print(config["problem"])

        print("\nWhat Spark does")
        print(config["spark_behavior"])

        print("\nWhy it matters")
        print(config["impact"])

        print("\nPossible solutions")
        for number, solution in enumerate(config["solutions"], start=1):
            print(f"{number}. {solution}")

        print("\nRecommended approach")
        print(config["recommended"])

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
    Read all CSV files created for the scenario and display the result.

    recursiveFileLookup is used because every micro-batch is stored in a
    separate batch_<id> directory. The output CSV files contain headers so they
    can also be opened directly without using Spark.
    """
    paths = scenario_paths(scenario_name)
    csv_files = list(paths["raw"].rglob("*.csv"))

    if not csv_files:
        print(f"No CSV output exists for: {scenario_name}")
        return None

    result_df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("recursiveFileLookup", "true")
        .csv(str(paths["raw"]))
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
        # coalesce(1) is used only to make this local classroom output easy to
        # open. Each batch folder contains one CSV data file with a header.
        # Large production datasets should normally keep parallel output files.
        (
            output_df.coalesce(1).write
            .format("csv")
            .option("header", "true")
            .mode("overwrite")
            .save(str(batch_output_path))
        )

        csv_files = list(batch_output_path.glob("part-*.csv"))
        if csv_files:
            readable_name = batch_output_path / f"customers_batch_{batch_id:020d}.csv"
            if readable_name.exists():
                readable_name.unlink()
            csv_files[0].rename(readable_name)
            print(f"Batch {batch_id} CSV written to: {readable_name}")
        else:
            print(f"Batch {batch_id} written to: {batch_output_path}")

    return write_one_batch


print("Customer schema-evolution helpers loaded.")
print("Run list_scenarios() to see the available demonstrations.")
