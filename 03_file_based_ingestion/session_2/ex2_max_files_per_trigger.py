"""
Session 2 - Exercise 2
Control file ingestion with maxFilesPerTrigger

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
# When many files arrive together, a production pipeline may need to control
# how quickly it consumes the backlog. Spark provides maxFilesPerTrigger.
#
# In this exercise, Spark processes at most ONE new file in each micro-batch.
# This is called input throttling.
#
# Student activity:
# 1. Start this script.
# 2. Copy orders_001.csv, orders_002.csv and orders_003.csv together.
# 3. Watch the console and raw-zone folder.
# 4. Notice that the backlog is processed gradually, one file per trigger.
#
# Important distinction:
# maxFilesPerTrigger limits files, not records. A single file may contain many rows.

BASE_DIR = Path(__file__).resolve().parent
EXERCISE_DIR = BASE_DIR / "runtime" / "ex2"
SOURCE_PATH = EXERCISE_DIR / "landing" / "orders" / "incoming"
RAW_PATH = EXERCISE_DIR / "raw_zone" / "orders"
CHECKPOINT_PATH = EXERCISE_DIR / "checkpoints" / "orders"

parser = argparse.ArgumentParser()
parser.add_argument("--reset", action="store_true", help="Delete old Ex2 runtime data before starting")
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
    .appName("Session2_Ex2_InputThrottling")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

orders_stream = (
    spark.readStream
    .format("csv")
    .option("header", "true")
    .option("maxFilesPerTrigger", 1)  # Production control: one new file per micro-batch.
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
    .queryName("ex2_controlled_file_ingestion")
    .format("parquet")
    .outputMode("append")
    .option("path", str(RAW_PATH))
    .option("checkpointLocation", str(CHECKPOINT_PATH))
    .trigger(processingTime="5 seconds")
    .start()
)

print("\nExercise 2 is running with maxFilesPerTrigger = 1.")
print(f"Landing folder : {SOURCE_PATH}")
print(f"Raw-zone folder: {RAW_PATH}")
print("Copy three files together and observe gradual processing. Press Ctrl+C to stop.\n")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping Exercise 2 safely...")
    query.stop()
finally:
    spark.stop()
