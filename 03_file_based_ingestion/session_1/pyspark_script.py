from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    current_timestamp,
    input_file_name,
)
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)


SOURCE_PATH = r"C:\Users\Sandeep\Desktop\file-based-ingestion\source"
RAW_PATH = r"C:\Users\Sandeep\Desktop\file-based-ingestion\raw_zone"
CHECKPOINT_PATH = r"C:\Users\Sandeep\Desktop\file-based-ingestion\checkpoint"

schema = StructType(
    [
        StructField("order_id", IntegerType(), False),
        StructField("customer_name", StringType(), True),
        StructField("product", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("order_status", StringType(), True),
    ]
)

spark.sparkContext.setLogLevel("ERROR")

orders_stream = (
    spark.readStream
    .format("csv")
    .option("header", "true")
    .schema(schema)
    .load(SOURCE_PATH)
)

raw_orders = (
    orders_stream
    .withColumn("source_file", input_file_name())
    .withColumn("ingestion_timestamp", current_timestamp())
)

# File Sink
query = (
    raw_orders.writeStream
    .format("csv")
    .outputMode("append")
    .option("path", RAW_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="5 seconds")
    .start()
)

# Console Sink
query = (
    raw_orders.writeStream
    .format("console")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="5 seconds")
    .start()
)

print(f"Monitoring source folder: {SOURCE_PATH}")
print(f"Writing raw data to: {RAW_PATH}")
print("Press Ctrl+C to stop the streaming job.")

query.awaitTermination()
