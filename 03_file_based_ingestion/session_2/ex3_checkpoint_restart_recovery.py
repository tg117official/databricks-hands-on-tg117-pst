"""
Session 2 - Exercise 3
Recover after restart by reusing the checkpoint

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
# A streaming job may stop because of deployment, machine restart or failure.
# The checkpoint stores query progress so Spark can continue from where it left off.
#
# Student activity:
# 1. Start with --reset.
# 2. Copy orders_001.csv and wait until it is processed.
# 3. Stop the job using Ctrl+C.
# 4. Copy orders_002.csv while the job is stopped.
# 5. Restart WITHOUT --reset.
# 6. Spark should process orders_002.csv and should not process orders_001.csv again.
#
# Key term: recovery.
# Recovery means restarting the same logical streaming query with the same checkpoint.

BASE_DIR = Path(__file__).resolve().parent
EXERCISE_DIR = BASE_DIR / "runtime" / "ex3"
SOURCE_PATH = EXERCISE_DIR / "landing" / "orders" / "incoming"
RAW_PATH = EXERCISE_DIR / "raw_zone" / "orders"
CHECKPOINT_PATH = EXERCISE_DIR / "checkpoints" / "orders"

parser = argparse.ArgumentParser()
parser.add_argument("--reset", action="store_true", help="Use only before the first run of Ex3")
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
    .appName("Session2_Ex3_CheckpointRecovery")
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
    .queryName("ex3_checkpoint_recovery")
    .format("parquet")
    .outputMode("append")
    .option("path", str(RAW_PATH))
    .option("checkpointLocation", str(CHECKPOINT_PATH))
    .trigger(processingTime="5 seconds")
    .start()
)

print("\nExercise 3 is running.")
print(f"Landing folder  : {SOURCE_PATH}")
print(f"Checkpoint folder: {CHECKPOINT_PATH}")
print("For the restart test, reuse the same checkpoint and do not pass --reset.\n")

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping Exercise 3 safely. The checkpoint is preserved.")
    query.stop()
finally:
    spark.stop()
