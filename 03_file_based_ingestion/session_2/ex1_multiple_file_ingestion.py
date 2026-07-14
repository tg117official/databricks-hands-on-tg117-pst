"""
Session 2 - Exercise 1
Ingest multiple arriving files into the raw zone

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
# In the previous session, we ingested one file. In this exercise, the same
# streaming job will process several files that arrive in the landing folder.
#
# Important terms:
# - Landing folder: the directory where new source files arrive.
# - Raw zone: append-only storage containing the data exactly as ingested,
#   along with technical metadata.
# - Micro-batch: a small batch of newly available files processed by Spark.
#
# Student activity:
# 1. Start this script.
# 2. Copy orders_001.csv and orders_002.csv into the printed landing folder.
# 3. Observe new Parquet files appearing in the raw-zone folder.
# 4. Keep the job running and copy orders_003.csv.
# 5. Observe that Spark processes only the newly arrived file.

BASE_DIR = Path(__file__).resolve().parent
EXERCISE_DIR = BASE_DIR / "runtime" / "ex1"
SOURCE_PATH = EXERCISE_DIR / "landing" / "orders" / "incoming"
RAW_PATH = EXERCISE_DIR / "raw_zone" / "orders"
CHECKPOINT_PATH = EXERCISE_DIR / "checkpoints" / "orders"

parser = argparse.ArgumentParser()
parser.add_argument("--reset", action="store_true", help="Delete old Ex1 runtime data before starting")
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
    .appName("Session2_Ex1_MultipleFiles")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# A production streaming pipeline should use an explicit schema.
# Schema inference is avoided because streaming inputs must be predictable.
orders_stream = (
    spark.readStream
    .format("csv")
    .option("header", "true")
    .schema(order_schema)
    .load(str(SOURCE_PATH))
)

# Technical metadata helps us understand where and when every raw record arrived.
raw_orders = (
    orders_stream
    .withColumn("source_file", input_file_name())
    .withColumn("ingestion_timestamp", current_timestamp())
)

query = (
    raw_orders.writeStream
    .queryName("ex1_multiple_file_ingestion")
    .format("parquet")
    .outputMode("append")
    .option("path", str(RAW_PATH))
    .option("checkpointLocation", str(CHECKPOINT_PATH))
    .trigger(processingTime="5 seconds")
    .start()
)

print("\nExercise 1 is running.")
print(f"Landing folder : {SOURCE_PATH}")
print(f"Raw-zone folder: {RAW_PATH}")
print("Copy multiple CSV files into the landing folder. Press Ctrl+C to stop.\n")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping Exercise 1 safely...")
    query.stop()
finally:
    spark.stop()
