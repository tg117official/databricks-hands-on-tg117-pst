"""
Session 2 - Exercise 5
Stop the stream when a malformed record arrives using FAILFAST

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
# Production pipelines need a clear rule for malformed input.
# FAILFAST immediately fails the micro-batch when Spark cannot parse a record.
#
# Student activity:
# 1. Start this script with --reset.
# 2. Copy orders_bad.csv into the landing folder.
# 3. Observe the parsing exception in the console.
# 4. Discuss when stopping the entire pipeline is acceptable.
#
# Note:
# orders_bad.csv contains the word "two" in the integer quantity column.
# A strict schema plus FAILFAST makes this a pipeline failure.

BASE_DIR = Path(__file__).resolve().parent
EXERCISE_DIR = BASE_DIR / "runtime" / "ex5"
SOURCE_PATH = EXERCISE_DIR / "landing" / "orders" / "incoming"
RAW_PATH = EXERCISE_DIR / "raw_zone" / "orders"
CHECKPOINT_PATH = EXERCISE_DIR / "checkpoints" / "orders"

parser = argparse.ArgumentParser()
parser.add_argument("--reset", action="store_true", help="Delete old Ex5 runtime data before starting")
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
    .appName("Session2_Ex5_FailFast")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

orders_stream = (
    spark.readStream
    .format("csv")
    .option("header", "true")
    .option("mode", "FAILFAST")  # Stop the query as soon as malformed input is found.
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
    .queryName("ex5_failfast_bad_record")
    .format("parquet")
    .outputMode("append")
    .option("path", str(RAW_PATH))
    .option("checkpointLocation", str(CHECKPOINT_PATH))
    .trigger(processingTime="5 seconds")
    .start()
)

print("\nExercise 5 is running in FAILFAST mode.")
print(f"Landing folder: {SOURCE_PATH}")
print("Copy orders_bad.csv and observe the streaming failure.\n")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping Exercise 5 manually...")
    query.stop()
finally:
    spark.stop()
