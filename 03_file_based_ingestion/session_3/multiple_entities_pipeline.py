from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name
from pyspark.sql.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
)

BASE_DIR = r"C:\Users\Sandeep\PycharmProjects\databricks-hands-on-tg117-pst\03_file_based_ingestion\session_3\base_dir"

spark = (
    SparkSession.builder
    .appName("MultiEntityFileIngestion")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------
# Entity 1: Orders
# ---------------------------------------------------------

orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("order_status", StringType(), True),
])

orders_stream = (
    spark.readStream
    .format("csv")
    .option("header", "true")
    .schema(orders_schema)
    .load(str(BASE_DIR / "landing" / "orders" / "incoming"))
)

orders_raw = (
    orders_stream
    .withColumn("source_file", input_file_name())
    .withColumn("ingestion_timestamp", current_timestamp())
)

orders_query = (
    orders_raw.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", str(BASE_DIR / "raw_zone" / "orders"))
    .option(
        "checkpointLocation",
        str(BASE_DIR / "checkpoints" / "orders"),
    )
    .trigger(processingTime="5 seconds")
    .start()
)

# ---------------------------------------------------------
# Entity 2: Customers
# ---------------------------------------------------------

customers_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("city", StringType(), True),
])

customers_stream = (
    spark.readStream
    .format("csv")
    .option("header", "true")
    .schema(customers_schema)
    .load(str(BASE_DIR / "landing" / "customers" / "incoming"))
)

customers_raw = (
    customers_stream
    .withColumn("source_file", input_file_name())
    .withColumn("ingestion_timestamp", current_timestamp())
)

customers_query = (
    customers_raw.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", str(BASE_DIR / "raw_zone" / "customers"))
    .option(
        "checkpointLocation",
        str(BASE_DIR / "checkpoints" / "customers"),
    )
    .trigger(processingTime="5 seconds")
    .start()
)

# ---------------------------------------------------------
# Entity 3: Products
# ---------------------------------------------------------

products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
])

products_stream = (
    spark.readStream
    .format("csv")
    .option("header", "true")
    .schema(products_schema)
    .load(str(BASE_DIR / "landing" / "products" / "incoming"))
)

products_raw = (
    products_stream
    .withColumn("source_file", input_file_name())
    .withColumn("ingestion_timestamp", current_timestamp())
)

products_query = (
    products_raw.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", str(BASE_DIR / "raw_zone" / "products"))
    .option(
        "checkpointLocation",
        str(BASE_DIR / "checkpoints" / "products"),
    )
    .trigger(processingTime="5 seconds")
    .start()
)

print("Orders, customers and products ingestion streams are running.")
print("Place files inside the respective incoming folders.")
print("Press Ctrl+C to stop the application.")

spark.streams.awaitAnyTermination()