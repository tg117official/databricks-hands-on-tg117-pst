"""
Session 2 - Exercise 4
Understand replay after deleting the checkpoint

This script is intentionally self-contained so that students can run and
understand one production-readiness concept at a time.
"""

from pathlib import Path
import argparse
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# -----------------------------------------------------------------------------
# LEARNING GOAL
# -----------------------------------------------------------------------------
# The checkpoint is not a temporary folder. It contains the progress history of
# the streaming query. If it is deleted, Spark forgets which files were processed.
#
# Student activity:
# 1. Start this script with --reset.
# 2. Copy orders_001.csv and wait for processing.
# 3. Stop the job.
# 4. Delete ONLY runtime/ex4/checkpoints/orders.
# 5. Keep the old source file and raw-zone output in place.
# 6. Restart the script without --reset.
# 7. Observe that Spark treats the existing source file as new input and appends it again.
#
# Key terms:
# - Replay: processing old input again.
# - Duplicate raw records: possible when progress state is lost.
# - Idempotency: rerunning an operation without changing the final result.
#   This append-only raw-zone example is not idempotent after checkpoint deletion.

BASE_DIR = Path(__file__).resolve().parent
EXERCISE_DIR = BASE_DIR / "runtime" / "ex4"
SOURCE_PATH = EXERCISE_DIR / "landing" / "orders" / "incoming"
RAW_PATH = EXERCISE_DIR / "raw_zone" / "orders"
CHECKPOINT_PATH = EXERCISE_DIR / "checkpoints" / "orders"

parser = argparse.ArgumentParser()
parser.add_argument("--reset", action="store_true", help="Delete all Ex4 runtime data before the first run")
args = parser.parse_args()

if args.reset and EXERCISE_DIR.exists():
    shutil.rmtree(EXERCISE_DIR)

SOURCE_PATH.mkdir(parents=True, exist_ok=True)
RAW_PATH.mkdir(parents=True, exist_ok=True)
CHECKPOINT_PATH.mkdir(parents=True, exist_ok=True)

order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("order_status", StringType(), True),
])

spark = (
    SparkSession.builder
    .appName("Session2_Ex4_CheckpointReplay")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

orders_stream = (
    spark.readStream
    .format("csv")
    .option("header", "true")
    .schema(order_schema)
    .load(str(SOURCE_PATH))
)

raw_orders = (
    orders_stream
    .withColumn("source_file", input_file_name())
    .withColumn("ingestion_timestamp", current_timestamp())
)

query = (
    raw_orders.writeStream
    .queryName("ex4_checkpoint_replay")
    .format("csv")
    .outputMode("append")
    .option("path", str(RAW_PATH))
    .option("checkpointLocation", str(CHECKPOINT_PATH))
    .trigger(processingTime="5 seconds")
    .start()
)

print("\nExercise 4 is running.")
print(f"Landing folder  : {SOURCE_PATH}")
print(f"Raw-zone folder : {RAW_PATH}")
print(f"Checkpoint folder: {CHECKPOINT_PATH}")
print("Delete only the checkpoint between runs to demonstrate replay.\n")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping Exercise 4 safely...")
    query.stop()
finally:
    spark.stop()
