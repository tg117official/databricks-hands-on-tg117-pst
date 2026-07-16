"""
Session 2 - Exercise 6
Capture malformed records using PERMISSIVE mode

This script is intentionally self-contained so that students can compare it
with Exercise 5 and understand two different production policies.
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
# PERMISSIVE mode attempts to keep the pipeline running when malformed input
# arrives. The original bad row can be captured in a special string column.
#
# Student activity:
# 1. Start this script with --reset.
# 2. Copy orders_bad.csv into the landing folder.
# 3. Inspect the raw-zone Parquet output using the inspection command in README.
# 4. Find the row where quantity is invalid and inspect _corrupt_record.
#
# Important:
# This is not a complete quarantine design. We are only demonstrating how the
# ingestion layer can retain evidence of malformed input without stopping.

BASE_DIR = Path(__file__).resolve().parent
EXERCISE_DIR = BASE_DIR / "runtime" / "ex6"
SOURCE_PATH = EXERCISE_DIR / "landing" / "orders" / "incoming"
RAW_PATH = EXERCISE_DIR / "raw_zone" / "orders"
CHECKPOINT_PATH = EXERCISE_DIR / "checkpoints" / "orders"

parser = argparse.ArgumentParser()
parser.add_argument("--reset", action="store_true", help="Delete old Ex6 runtime data before starting")
args = parser.parse_args()

if args.reset and EXERCISE_DIR.exists():
    shutil.rmtree(EXERCISE_DIR)

SOURCE_PATH.mkdir(parents=True, exist_ok=True)
RAW_PATH.mkdir(parents=True, exist_ok=True)
CHECKPOINT_PATH.mkdir(parents=True, exist_ok=True)

# The corrupt-record field must be part of the supplied schema.
order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("order_status", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])

spark = (
    SparkSession.builder
    .appName("Session2_Ex6_PermissiveBadRecords")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

orders_stream = (
    spark.readStream
    .format("csv")
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
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
    .queryName("ex6_permissive_bad_record")
    .format("csv")
    .outputMode("append")
    .option("path", str(RAW_PATH))
    .option("checkpointLocation", str(CHECKPOINT_PATH))
    .trigger(processingTime="5 seconds")
    .start()
)

print("\nExercise 6 is running in PERMISSIVE mode.")
print(f"Landing folder : {SOURCE_PATH}")
print(f"Raw-zone folder: {RAW_PATH}")
print("Copy orders_bad.csv and then inspect _corrupt_record. Press Ctrl+C to stop.\n")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping Exercise 6 safely...")
    query.stop()
finally:
    spark.stop()
