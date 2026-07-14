"""
Utility: inspect Parquet output produced by any exercise.

Example:
    python inspect_raw_zone.py --exercise ex1
"""

from pathlib import Path
import argparse

from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parent

parser = argparse.ArgumentParser()
parser.add_argument(
    "--exercise",
    required=True,
    choices=["ex1", "ex2", "ex3", "ex4", "ex5", "ex6", "ex7"],
)
args = parser.parse_args()

raw_path = BASE_DIR / "runtime" / args.exercise / "raw_zone" / "orders"

spark = (
    SparkSession.builder
    .appName(f"Inspect_{args.exercise}_RawZone")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

if not raw_path.exists() or not any(raw_path.iterdir()):
    print(f"No output is available at: {raw_path}")
else:
    df = spark.read.parquet(str(raw_path))
    print(f"Raw-zone output for {args.exercise}: {raw_path}")
    df.orderBy("order_id").show(truncate=False)
    print(f"Total raw records: {df.count()}")

spark.stop()
