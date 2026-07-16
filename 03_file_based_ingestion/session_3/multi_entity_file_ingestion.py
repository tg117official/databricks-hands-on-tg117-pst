"""
Session 3: Reusable Multi-Entity File Ingestion

One Spark application starts independent file-streaming queries for orders,
customers, and products. The same functions are reused for every entity.

Each micro-batch is written to its own directory, for example:

raw_zone/orders/pipeline_v1/batches/batch_00000000000000000000/

Keep each entity's checkpoint directory safe. If the pipeline definition is
intentionally reset, change PIPELINE_VERSION so that new checkpoint and output
locations do not collide with the previous pipeline version.
"""

from pathlib import Path
from typing import Any, Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


# =============================================================================
# 1. COMMON PIPELINE SETTINGS
# =============================================================================

BASE_DIR = Path(__file__).resolve().parent
PIPELINE_VERSION = "pipeline_v1"
TRIGGER_INTERVAL = "5 seconds"
MAX_FILES_PER_TRIGGER = 2


# =============================================================================
# 2. ENTITY SCHEMAS
# =============================================================================

orders_schema = StructType(
    [
        StructField("order_id", IntegerType(), False),
        StructField("customer_id", IntegerType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("order_status", StringType(), True),
    ]
)

customers_schema = StructType(
    [
        StructField("customer_id", IntegerType(), False),
        StructField("customer_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("email", StringType(), True),
    ]
)

products_schema = StructType(
    [
        StructField("product_id", IntegerType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
    ]
)


# =============================================================================
# 3. ENTITY CONFIGURATION
# =============================================================================

# Only values that differ by entity are defined here.
# The ingestion logic is written once and reused through functions and a loop.
#
# Every entity must have its own checkpoint directory. Two streaming queries
# must never share the same checkpoint directory.
ENTITY_CONFIGS: dict[str, dict[str, Any]] = {
    "orders": {
        "schema": orders_schema,
        "source_path": r"C:\Users\Sandeep\PycharmProjects\databricks-hands-on-tg117-pst\03_file_based_ingestion\session_3\base_dir\landing\orders\incoming",
        "raw_path": r"C:\Users\Sandeep\PycharmProjects\databricks-hands-on-tg117-pst\03_file_based_ingestion\session_3\base_dir\raw_zone\orders",
        "checkpoint_path": r"C:\Users\Sandeep\PycharmProjects\databricks-hands-on-tg117-pst\03_file_based_ingestion\session_3\base_dir\checkpoints\orders",
    },
    "customers": {
        "schema": customers_schema,
        "source_path": r"C:\Users\Sandeep\PycharmProjects\databricks-hands-on-tg117-pst\03_file_based_ingestion\session_3\base_dir\landing\customers\incoming",
        "raw_path": r"C:\Users\Sandeep\PycharmProjects\databricks-hands-on-tg117-pst\03_file_based_ingestion\session_3\base_dir\raw_zone\customers",
        "checkpoint_path": r"C:\Users\Sandeep\PycharmProjects\databricks-hands-on-tg117-pst\03_file_based_ingestion\session_3\base_dir\checkpoints\customers",
    },
    "products": {
        "schema": products_schema,
        "source_path": r"C:\Users\Sandeep\PycharmProjects\databricks-hands-on-tg117-pst\03_file_based_ingestion\session_3\base_dir\landing\products\incoming",
        "raw_path": r"C:\Users\Sandeep\PycharmProjects\databricks-hands-on-tg117-pst\03_file_based_ingestion\session_3\base_dir\raw_zone\products",
        "checkpoint_path": r"C:\Users\Sandeep\PycharmProjects\databricks-hands-on-tg117-pst\03_file_based_ingestion\session_3\base_dir\checkpoints\products",
    },
}


# =============================================================================
# 4. REUSABLE FUNCTIONS
# =============================================================================

# def create_required_directories() -> None:
#     """Create landing, raw, and checkpoint directories for every entity."""
#     for config in ENTITY_CONFIGS.values():
#         config["raw_path"].mkdir(parents=True, exist_ok=True)
#         config["checkpoint_path"].mkdir(parents=True, exist_ok=True)

def create_spark_session() -> SparkSession:
    """Create one SparkSession for the complete multi-entity application."""
    spark = (
        SparkSession.builder
        .appName("ReusableMultiEntityFileIngestion")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark

def build_streaming_dataframe(
    spark: SparkSession,
    entity_name: str,
    config: dict[str, Any],
) -> DataFrame:
    """
    Create the streaming DataFrame for one entity.

    This same function works for all entities because the schema and source
    path are received through the configuration dictionary.
    """
    source_df = (
        spark.readStream
        .format("csv")
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("maxFilesPerTrigger", MAX_FILES_PER_TRIGGER)
        .schema(config["schema"])
        .load(str(config["source_path"]))
    )
    # Technical metadata improves traceability in the raw zone.
    return (
        source_df
        .withColumn("source_entity", lit(entity_name))
        .withColumn("source_file", input_file_name())
        .withColumn("ingestion_timestamp", current_timestamp())
    )

def create_batch_writer(
    entity_name: str,
    raw_batches_path: Path,
) -> Callable[[DataFrame, int], None]:
    """
    Return the foreachBatch function for one entity.

    A closure is used so the returned function remembers the entity name and
    the correct raw-zone path.
    """
    def write_one_batch(batch_df: DataFrame, batch_id: int) -> None:
        """
        Write one micro-batch to its own folder.

        Example:
        batch_00000000000000000000/
        batch_00000000000000000001/

        overwrite mode makes a retry of the same batch ID replace the same
        batch folder instead of silently creating a second folder.
        """
        if batch_df.isEmpty():
            print(f"[{entity_name}] Batch {batch_id} had no rows.")
            return
        batch_folder = f"batch_{batch_id:020d}"
        batch_output_path = raw_batches_path / batch_folder
        output_df = (
            batch_df
            .withColumn("streaming_batch_id", lit(batch_id))
            .withColumn("batch_written_at", current_timestamp())
        )
        # Do not use coalesce(1) here. A batch folder may correctly contain
        # multiple Parquet part files because Spark writes in parallel.
        (
            output_df.write
            .format("csv")
            .mode("overwrite")
            .save(str(batch_output_path))
        )
        print(
            f"[{entity_name}] Batch {batch_id} written successfully:\n"
            f"  {batch_output_path}"
        )
    return write_one_batch

def start_entity_query(
    spark: SparkSession,
    entity_name: str,
    config: dict[str, Any],
) -> StreamingQuery:
    """Build and start one independent streaming query."""
    streaming_df = build_streaming_dataframe(
        spark=spark,
        entity_name=entity_name,
        config=config,
    )
    batch_writer = create_batch_writer(
        entity_name=entity_name,
        raw_batches_path=config["raw_path"],
    )
    query = (
        streaming_df.writeStream
        .queryName(f"{entity_name}_raw_ingestion")
        .outputMode("append")
        .foreachBatch(batch_writer)
        .option("checkpointLocation", str(config["checkpoint_path"]))
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )
    print(
        f"\nStarted query: {query.name}\n"
        f"  Source     : {config['source_path']}\n"
        f"  Raw batches: {config['raw_path']}\n"
        f"  Checkpoint : {config['checkpoint_path']}"
    )
    return query

def stop_all_queries(queries: list[StreamingQuery]) -> None:
    """Stop every active query during a controlled shutdown."""
    for query in queries:
        if query.isActive:
            print(f"Stopping query: {query.name}")
            query.stop()

# =============================================================================
# 5. MAIN PROGRAM
# =============================================================================

def main() -> None:
    """
    Start one query per configured entity.

    To add another entity later, define its schema and add one configuration
    entry. The read and write logic does not need to be copied again.
    """
    # create_required_directories()
    # spark = create_spark_session()
    active_queries: list[StreamingQuery] = []
    try:
        for entity_name, config in ENTITY_CONFIGS.items():
            active_queries.append(
                start_entity_query(
                    spark=spark,
                    entity_name=entity_name,
                    config=config,
                )
            )
        print("\n" + "=" * 75)
        print("MULTI-ENTITY FILE INGESTION IS RUNNING")
        print("=" * 75)
        print("Place CSV files into the matching incoming folder:")
        for entity_name, config in ENTITY_CONFIGS.items():
            print(f"  {entity_name:<10} -> {config['source_path']}")
        print("\nPress Ctrl+C to stop all streaming queries.")
        print(active_queries)
        # The application remains alive while the queries run. If any query
        # fails, awaitAnyTermination surfaces that failure to this application.
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nShutdown requested by the user.")
    except Exception as error:
        print(f"\nThe streaming application failed: {error}")
        raise
    finally:
        # stop_all_queries(active_queries)
        # spark.stop()
        print("Spark application stopped.")


if __name__ == "__main__":
    main()
