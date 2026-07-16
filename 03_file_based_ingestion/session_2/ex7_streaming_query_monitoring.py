"""
Session 2 - Exercise 7
Monitor a running Structured Streaming query

This script is intentionally self-contained. It prints selected operational
metrics so students can understand basic streaming observability.
"""

from pathlib import Path
import argparse
import json
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# -----------------------------------------------------------------------------
# LEARNING GOAL
# -----------------------------------------------------------------------------
# A production streaming job should not be treated as a black box.
# Spark exposes status and progress information for every active query.
#
# Student activity:
# 1. Start this script with --reset.
# 2. Copy one file, then another file.
# 3. Observe batchId, numInputRows, inputRowsPerSecond and processedRowsPerSecond.
# 4. Notice that some triggers may contain zero input rows when no new file arrives.
#
# Key terms:
# - Observability: the ability to understand what a running system is doing.
# - Throughput: how many rows are processed in a period of time.
# - Latency: how long processing takes after data becomes available.

BASE_DIR = Path(__file__).resolve().parent
EXERCISE_DIR = BASE_DIR / "runtime" / "ex7"
SOURCE_PATH = EXERCISE_DIR / "landing" / "orders" / "incoming"
RAW_PATH = EXERCISE_DIR / "raw_zone" / "orders"
CHECKPOINT_PATH = EXERCISE_DIR / "checkpoints" / "orders"

parser = argparse.ArgumentParser()
parser.add_argument("--reset", action="store_true", help="Delete old Ex7 runtime data before starting")
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
    .appName("Session2_Ex7_QueryMonitoring")
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
    .queryName("ex7_orders_raw_monitoring")
    .format("parquet")
    .outputMode("append")
    .option("path", str(RAW_PATH))
    .option("checkpointLocation", str(CHECKPOINT_PATH))
    .trigger(processingTime="5 seconds")
    .start()
)

print("\nExercise 7 is running with monitoring enabled.")
print(f"Landing folder: {SOURCE_PATH}")
print("Copy files and watch the progress summary below. Press Ctrl+C to stop.\n")

try:
    while query.isActive:
        # Wait up to five seconds. The loop continues while the query remains active.
        query.awaitTermination(5)
        print("Query status:")
        print(json.dumps(query.status, indent=2))
        progress = query.lastProgress
        if progress:
            summary = {
                "batchId": progress.get("batchId"),
                "timestamp": progress.get("timestamp"),
                "numInputRows": progress.get("numInputRows"),
                "inputRowsPerSecond": progress.get("inputRowsPerSecond"),
                "processedRowsPerSecond": progress.get("processedRowsPerSecond"),
                "durationMs": progress.get("durationMs"),
            }
            print("Latest progress:")
            print(json.dumps(summary, indent=2))
        else:
            print("No completed micro-batch is available yet.")
        print("-" * 70)
except KeyboardInterrupt:
    print("\nStopping Exercise 7 safely...")
    query.stop()
finally:
    spark.stop()
