"""
Reusable helpers for customer schema-evolution demonstrations in the PySpark shell.

Start PySpark from this project folder, then load the helpers:

    exec(open("customer_schema_evolution_shell_helpers.py", encoding="utf-8").read())

Nothing runs automatically. Each problem and solution can be demonstrated one
step at a time from the shell.
"""

from pathlib import Path
import shutil
from typing import Any, Callable

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    coalesce,
    col,
    current_timestamp,
    input_file_name,
    lit,
    trim,
)
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


# -----------------------------------------------------------------------------
# Project folders
# -----------------------------------------------------------------------------

# Start the PySpark shell from the folder containing this helper file.
PROJECT_DIR = Path.cwd()
RUNTIME_DIR = PROJECT_DIR / "schema_evolution_runtime"
SAMPLE_DIR = PROJECT_DIR / "sample_files"
LANDING_ROOT = RUNTIME_DIR / "landing" / "customers"
RAW_ROOT = RUNTIME_DIR / "raw_zone" / "customers"
CHECKPOINT_ROOT = RUNTIME_DIR / "checkpoints" / "customers"


# -----------------------------------------------------------------------------
# Customer schemas
# -----------------------------------------------------------------------------

# Original customer contract.
# customer_id, customer_name, city, email
CUSTOMER_SCHEMA_V1 = StructType(
    [
        StructField("customer_id", IntegerType(), True),
        StructField("customer_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("email", StringType(), True),
    ]
)

# V2 adds phone_number at the end.
# customer_id, customer_name, city, email, phone_number
CUSTOMER_SCHEMA_V2 = StructType(
    [
        StructField("customer_id", IntegerType(), True),
        StructField("customer_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone_number", StringType(), True),
    ]
)

# The source has the same fields as V2, but email and city arrive in a
# different order. This schema matches the source file exactly.
CUSTOMER_SCHEMA_V2_REORDERED_SOURCE = StructType(
    [
        StructField("customer_id", IntegerType(), True),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("city", StringType(), True),
        StructField("phone_number", StringType(), True),
    ]
)

# The source has renamed two fields. This schema matches the source names.
CUSTOMER_SCHEMA_RENAMED_SOURCE = StructType(
    [
        StructField("customer_id", IntegerType(), True),
        StructField("full_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("email_address", StringType(), True),
        StructField("phone_number", StringType(), True),
    ]
)

# V3 stores customer_id as a string so both 101 and C110 are valid values.
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
# Problem-solution pairs
# -----------------------------------------------------------------------------

DEMO_PAIRS: dict[str, tuple[str, str]] = {
    "added_column": (
        "added_column_old_schema",
        "added_column_new_schema",
    ),
    "old_file_after_upgrade": (
        "old_file_new_schema",
        "old_file_new_schema_handled",
    ),
    "reordered_columns": (
        "reordered_columns_unsafe",
        "reordered_columns_mapped",
    ),
    "renamed_columns": (
        "renamed_columns_rejected",
        "renamed_columns_mapped",
    ),
    "datatype_change": (
        "datatype_change_old_schema",
        "datatype_change_new_schema",
    ),
}


# -----------------------------------------------------------------------------
# Scenario configuration
# -----------------------------------------------------------------------------

# transform controls the practical fix applied after reading the file:
#
# none
#     Keep the parsed columns as they are.
#
# fill_phone_default
#     Replace a missing phone_number with NOT_PROVIDED.
#
# reorder_to_canonical
#     Read the source order correctly, then select the canonical column order.
#
# rename_to_canonical
#     Map full_name and email_address back to the canonical names.
#
# required_columns lists fields that must not be null or blank before writing.
SCENARIOS: dict[str, dict[str, Any]] = {
    "baseline": {
        "kind": "baseline",
        "title": "Baseline file matches schema V1",
        "file_name": "customers_01_baseline.csv",
        "schema": CUSTOMER_SCHEMA_V1,
        "schema_version": "v1",
        "enforce_schema": True,
        "transform": "none",
        "handling": "none",
        "required_columns": [],
        "problem": "There is no schema mismatch in this file.",
        "spark_behavior": "Spark reads all four fields correctly.",
        "solution": "Use this output as the reference for later comparisons.",
        "expected": "customer_id, customer_name, city, and email are visible.",
    },
    "added_column_old_schema": {
        "kind": "problem",
        "title": "Problem: V2 file arrives while the query still uses V1",
        "file_name": "customers_02_added_phone_old_schema.csv",
        "schema": CUSTOMER_SCHEMA_V1,
        "schema_version": "v1",
        "enforce_schema": True,
        "transform": "none",
        "handling": "old_schema_used",
        "required_columns": [],
        "problem": (
            "The source added phone_number, but the running query still knows "
            "only the four V1 fields."
        ),
        "spark_behavior": (
            "The query can complete, but phone_number is not part of the "
            "output. The extra value is effectively lost."
        ),
        "solution": (
            "Update the query to schema V2, keep the new field nullable, and "
            "restart with a controlled output and checkpoint version."
        ),
        "expected": "The output does not contain phone_number.",
    },
    "added_column_new_schema": {
        "kind": "solution",
        "title": "Solution: Upgrade the query to schema V2",
        "file_name": "customers_02_added_phone_old_schema.csv",
        "schema": CUSTOMER_SCHEMA_V2,
        "schema_version": "v2",
        "enforce_schema": True,
        "transform": "none",
        "handling": "schema_upgraded_to_v2",
        "required_columns": [],
        "problem": "The V1 query could not expose phone_number.",
        "spark_behavior": (
            "The V2 schema now includes phone_number, so Spark captures the "
            "new field."
        ),
        "solution": (
            "Deploy the V2 schema and test both old and new files before the "
            "old contract is retired."
        ),
        "expected": "phone_number is visible in the output CSV.",
    },
    "old_file_new_schema": {
        "kind": "problem",
        "title": "Problem: An old V1 file arrives after the V2 upgrade",
        "file_name": "customers_03_old_file_new_schema.csv",
        "schema": CUSTOMER_SCHEMA_V2,
        "schema_version": "v2",
        "enforce_schema": True,
        "transform": "none",
        "handling": "missing_field_left_null",
        "required_columns": [],
        "problem": (
            "The query expects phone_number, but an older producer still sends "
            "only four fields."
        ),
        "spark_behavior": (
            "Spark keeps the row and stores phone_number as null because the "
            "missing field is at the end of the record."
        ),
        "solution": (
            "Allow a short compatibility window. Keep null if it is meaningful, "
            "or apply an agreed default and monitor how often it is used."
        ),
        "expected": "phone_number is null.",
    },
    "old_file_new_schema_handled": {
        "kind": "solution",
        "title": "Solution: Apply a temporary default for the missing field",
        "file_name": "customers_03_old_file_new_schema.csv",
        "schema": CUSTOMER_SCHEMA_V2,
        "schema_version": "v2",
        "enforce_schema": True,
        "transform": "fill_phone_default",
        "handling": "missing_phone_defaulted",
        "required_columns": [],
        "problem": "Older files do not contain phone_number.",
        "spark_behavior": (
            "Spark first reads phone_number as null. The compatibility step "
            "then replaces null with NOT_PROVIDED."
        ),
        "solution": (
            "Use the default only during the agreed transition period. Remove "
            "it once every producer sends the V2 format."
        ),
        "expected": "phone_number contains NOT_PROVIDED instead of null.",
    },
    "reordered_columns_unsafe": {
        "kind": "problem",
        "title": "Problem: CSV columns are reordered but V2 order is forced",
        "file_name": "customers_04_reordered_columns.csv",
        "schema": CUSTOMER_SCHEMA_V2,
        "schema_version": "v2",
        "enforce_schema": True,
        "transform": "none",
        "handling": "positional_mapping",
        "required_columns": [],
        "problem": (
            "The file sends email before city, while schema V2 expects city "
            "before email."
        ),
        "spark_behavior": (
            "Because both values are strings, the query can succeed and place "
            "the values in the wrong fields."
        ),
        "solution": (
            "Do not rely on the old order. Match the source contract explicitly, "
            "then select columns into the canonical order."
        ),
        "expected": "city contains an email and email contains a city.",
    },
    "reordered_columns_mapped": {
        "kind": "solution",
        "title": "Solution: Read the source order and select canonical order",
        "file_name": "customers_04_reordered_columns.csv",
        "schema": CUSTOMER_SCHEMA_V2_REORDERED_SOURCE,
        "schema_version": "v2",
        "enforce_schema": False,
        "transform": "reorder_to_canonical",
        "handling": "source_order_mapped_to_canonical",
        "required_columns": [],
        "problem": "The source column order differs from the canonical order.",
        "spark_behavior": (
            "Spark first reads the file using the real source order. The query "
            "then selects city and email in the canonical order."
        ),
        "solution": (
            "Keep the mapping explicit and versioned. Reject any unrecognised "
            "header instead of guessing the order."
        ),
        "expected": "city and email appear in the correct columns.",
    },
    "renamed_columns_rejected": {
        "kind": "problem",
        "title": "Problem: The source renames customer_name and email",
        "file_name": "customers_05_renamed_columns.csv",
        "schema": CUSTOMER_SCHEMA_V2,
        "schema_version": "v2",
        "enforce_schema": False,
        "transform": "none",
        "handling": "header_validation",
        "required_columns": [],
        "problem": (
            "The source sends full_name and email_address, but the contract "
            "still expects customer_name and email."
        ),
        "spark_behavior": (
            "Header validation rejects the file. This is safer than silently "
            "placing values by position."
        ),
        "solution": (
            "If the rename is approved, read the new source names and map them "
            "back to the canonical names used by downstream systems."
        ),
        "expected": "The query fails with a header mismatch and writes no CSV.",
    },
    "renamed_columns_mapped": {
        "kind": "solution",
        "title": "Solution: Map the renamed fields to canonical names",
        "file_name": "customers_05_renamed_columns.csv",
        "schema": CUSTOMER_SCHEMA_RENAMED_SOURCE,
        "schema_version": "v2",
        "enforce_schema": False,
        "transform": "rename_to_canonical",
        "handling": "renamed_fields_mapped",
        "required_columns": [],
        "problem": "The source uses new names for two existing fields.",
        "spark_behavior": (
            "Spark reads full_name and email_address, then aliases them to "
            "customer_name and email before writing."
        ),
        "solution": (
            "Use explicit aliases during the migration. Remove the mapping only "
            "after every consumer adopts the new contract."
        ),
        "expected": "The output uses customer_name and email again.",
    },
    "datatype_change_old_schema": {
        "kind": "problem",
        "title": "Problem: customer_id becomes alphanumeric under IntegerType",
        "file_name": "customers_06_alphanumeric_id.csv",
        "schema": CUSTOMER_SCHEMA_V2,
        "schema_version": "v2",
        "enforce_schema": True,
        "transform": "none",
        "handling": "integer_schema_used",
        "required_columns": [],
        "problem": (
            "The source starts sending C110, but schema V2 still defines "
            "customer_id as an integer."
        ),
        "spark_behavior": (
            "Under permissive parsing, customer_id can become null while the "
            "query continues."
        ),
        "solution": (
            "Move the identifier to StringType and add a mandatory-key check so "
            "a missing customer_id cannot be written silently."
        ),
        "expected": "customer_id is null.",
    },
    "datatype_change_new_schema": {
        "kind": "solution",
        "title": "Solution: Use StringType and validate the business key",
        "file_name": "customers_06_alphanumeric_id.csv",
        "schema": CUSTOMER_SCHEMA_V3,
        "schema_version": "v3",
        "enforce_schema": True,
        "transform": "none",
        "handling": "string_id_with_required_key_check",
        "required_columns": ["customer_id"],
        "problem": "IntegerType cannot preserve alphanumeric identifiers.",
        "spark_behavior": (
            "Schema V3 stores C110 as a string. The batch writer also checks "
            "that customer_id is not null or blank."
        ),
        "solution": (
            "Use string as the canonical identifier type and migrate downstream "
            "tables, joins, and APIs in a controlled release."
        ),
        "expected": "customer_id contains C110 and passes validation.",
    },
}


# Queries are stored by scenario name so they can be inspected and stopped.
ACTIVE_QUERIES: dict[str, StreamingQuery] = {}


# -----------------------------------------------------------------------------
# Shell-friendly functions
# -----------------------------------------------------------------------------

def list_demo_pairs() -> None:
    """Show the recommended problem-solution pairs."""
    print("Available problem-solution pairs:\n")
    for number, (pair_name, names) in enumerate(DEMO_PAIRS.items(), start=1):
        problem_name, solution_name = names
        print(f"{number}. {pair_name}")
        print(f"   Problem : {problem_name}")
        print(f"   Solution: {solution_name}")


def list_scenarios() -> None:
    """Show all scenario names in the recommended order."""
    print("Available scenarios:\n")
    for number, (name, config) in enumerate(SCENARIOS.items(), start=1):
        print(f"{number}. {name}")
        print(f"   [{config['kind']}] {config['title']}")


def explain_scenario(scenario_name: str) -> None:
    """Explain the problem, Spark behaviour, fix, and expected output."""
    config = _get_scenario(scenario_name)
    print(f"\n{config['title']}")
    print("-" * len(config["title"]))
    print(f"Problem         : {config['problem']}")
    print(f"What Spark does : {config['spark_behavior']}")
    print(f"Practical fix   : {config['solution']}")
    print(f"Expected result : {config['expected']}")
    print(f"Schema version  : {config['schema_version']}")
    print(f"Handling        : {config['handling']}")
    print(f"Sample file     : {SAMPLE_DIR / config['file_name']}")


def show_pair(pair_name: str) -> None:
    """Show the problem and solution scenario names for one pair."""
    problem_name, solution_name = _get_pair(pair_name)
    print(f"\nPair: {pair_name}")
    print(f"Problem : {problem_name}")
    print(f"Solution: {solution_name}")
    print("\nRun the problem first, inspect it, then run the solution.")


def show_pair_commands(pair_name: str) -> None:
    """Print copy-paste commands for one problem-solution pair."""
    problem_name, solution_name = _get_pair(pair_name)
    print(
        f'''\n# Problem\nscenario = "{problem_name}"\nprepare_scenario(scenario)\nexplain_scenario(scenario)\nshow_input_file(scenario)\nq = start_customer_stream(scenario)\narrive_file(scenario)\nprocess_available_data_safely(scenario)\nshow_raw_output(scenario)\nstop_customer_stream(scenario)\n\n# Solution\nscenario = "{solution_name}"\nprepare_scenario(scenario)\nexplain_scenario(scenario)\nshow_input_file(scenario)\nq = start_customer_stream(scenario)\narrive_file(scenario)\nprocess_available_data_safely(scenario)\nshow_raw_output(scenario)\nstop_customer_stream(scenario)\n\n# Compare both outputs\ncompare_pair_outputs("{pair_name}")'''
    )


def show_input_file(scenario_name: str) -> None:
    """Print the exact CSV content used by a scenario."""
    config = _get_scenario(scenario_name)
    sample_file = SAMPLE_DIR / config["file_name"]
    print(f"\n{sample_file}\n")
    print(sample_file.read_text(encoding="utf-8"))


def prepare_scenario(scenario_name: str) -> dict[str, Path]:
    """Create clean landing, raw, and checkpoint folders for one scenario."""
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
    """Start one customer stream for the selected problem or solution."""
    config = _get_scenario(scenario_name)
    paths = scenario_paths(scenario_name)

    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)

    existing = ACTIVE_QUERIES.get(scenario_name)
    if existing is not None and existing.isActive:
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

    canonical_stream = _apply_transform(customer_stream, config["transform"])

    enriched_stream = (
        canonical_stream
        .withColumn("source_file", input_file_name())
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("schema_version", lit(config["schema_version"]))
        .withColumn("handling_applied", lit(config["handling"]))
        .withColumn("scenario_name", lit(scenario_name))
    )

    batch_writer = _create_batch_writer(
        raw_path=paths["raw"],
        required_columns=config["required_columns"],
    )

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
    print(f"Next command: arrive_file('{scenario_name}')")
    return query


def arrive_file(scenario_name: str) -> Path:
    """Copy the complete sample file into the monitored landing folder."""
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
    """Wait until Spark finishes all files currently available."""
    query = get_query(scenario_name)
    query.processAllAvailable()
    print(f"All currently available data was processed for: {scenario_name}")


def process_available_data_safely(scenario_name: str) -> bool:
    """Process available files and keep the shell usable after an error."""
    try:
        process_available_data(scenario_name)
        return True
    except Exception as error:
        print(f"Processing failed for: {scenario_name}")
        print(error)
        return False


def show_raw_output(scenario_name: str) -> DataFrame | None:
    """Read and display all CSV batch output for one scenario."""
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
    result_df.show()
    return result_df


def compare_pair_outputs(pair_name: str) -> None:
    """Display the problem output followed by the solution output."""
    problem_name, solution_name = _get_pair(pair_name)
    print(f"\n{'=' * 80}\nPROBLEM OUTPUT: {problem_name}\n{'=' * 80}")
    show_raw_output(problem_name)
    print(f"\n{'=' * 80}\nSOLUTION OUTPUT: {solution_name}\n{'=' * 80}")
    show_raw_output(solution_name)


def show_query_status(scenario_name: str) -> None:
    """Display status, latest progress, and any query exception."""
    query = get_query(scenario_name)
    print(f"Query name : {query.name}")
    print(f"Is active  : {query.isActive}")
    print(f"Status     : {query.status}")
    print(f"Exception  : {query.exception()}")
    print(f"Last batch : {query.lastProgress}")


def stop_customer_stream(scenario_name: str, quiet: bool = False) -> None:
    """Stop the selected query if it exists and is active."""
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
    """Return the landing, raw, and checkpoint paths for one scenario."""
    _get_scenario(scenario_name)
    return {
        "landing": LANDING_ROOT / scenario_name / "incoming",
        "raw": RAW_ROOT / scenario_name / "batches",
        "checkpoint": CHECKPOINT_ROOT / scenario_name,
    }


def get_query(scenario_name: str) -> StreamingQuery:
    """Return the query object for a scenario."""
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


def _get_pair(pair_name: str) -> tuple[str, str]:
    if pair_name not in DEMO_PAIRS:
        valid = ", ".join(DEMO_PAIRS)
        raise ValueError(f"Unknown pair '{pair_name}'. Choose one of: {valid}")
    return DEMO_PAIRS[pair_name]


def _apply_transform(df: DataFrame, transform_name: str) -> DataFrame:
    """Apply the selected compatibility or mapping step."""
    if transform_name == "none":
        return df

    if transform_name == "fill_phone_default":
        return df.withColumn(
            "phone_number",
            coalesce(col("phone_number"), lit("NOT_PROVIDED")),
        )

    if transform_name == "reorder_to_canonical":
        return df.select(
            "customer_id",
            "customer_name",
            "city",
            "email",
            "phone_number",
        )

    if transform_name == "rename_to_canonical":
        return df.select(
            col("customer_id"),
            col("full_name").alias("customer_name"),
            col("city"),
            col("email_address").alias("email"),
            col("phone_number"),
        )

    raise ValueError(f"Unsupported transform: {transform_name}")


def _create_batch_writer(
    raw_path: Path,
    required_columns: list[str],
) -> Callable[[DataFrame, int], None]:
    """Create a CSV writer that keeps every micro-batch in its own folder."""

    def write_one_batch(batch_df: DataFrame, batch_id: int) -> None:
        # Mandatory fields are checked before writing the batch.
        for column_name in required_columns:
            invalid_exists = (
                batch_df
                .filter(
                    col(column_name).isNull()
                    | (trim(col(column_name).cast("string")) == "")
                )
                .limit(1)
                .count()
                > 0
            )
            if invalid_exists:
                raise ValueError(
                    f"Batch {batch_id} rejected: required field "
                    f"'{column_name}' is null or blank."
                )

        batch_output_path = raw_path / f"batch_{batch_id:020d}"

        output_df = (
            batch_df
            .withColumn("streaming_batch_id", lit(batch_id))
            .withColumn("batch_written_at", current_timestamp())
        )

        # One CSV file is used only to make the local demo easy to open.
        # Large production batches should normally keep parallel output files.
        (
            output_df.coalesce(1).write
            .format("csv")
            .option("header", "true")
            .mode("overwrite")
            .save(str(batch_output_path))
        )

        csv_files = list(batch_output_path.glob("part-*.csv"))
        if csv_files:
            readable_name = (
                batch_output_path
                / f"customers_batch_{batch_id:020d}.csv"
            )
            if readable_name.exists():
                readable_name.unlink()
            csv_files[0].rename(readable_name)
            print(f"Batch {batch_id} CSV written to: {readable_name}")
        else:
            print(f"Batch {batch_id} written to: {batch_output_path}")

    return write_one_batch


print("Customer schema-evolution helpers loaded.")
print("Run list_demo_pairs() to see the problem-solution demonstrations.")
