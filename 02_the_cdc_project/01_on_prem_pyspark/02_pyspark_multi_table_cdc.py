# Start with:
# pyspark --packages mysql:mysql-connector-java:8.0.28
#
# Or submit with:
# spark-submit --packages mysql:mysql-connector-java:8.0.28 02_pyspark_multi_table_cdc.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, max as spark_max

spark = SparkSession.builder.appName("mysql-multi-table-query-cdc").getOrCreate()

# ---------------------------------------------------------
# 1) Connection configs
# ---------------------------------------------------------
mysql_url = "jdbc:mysql://localhost:3306/inventory"
mysql_props = {
    "user": "root",
    "password": "root",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Local output folder for demo.
# For ADLS in real project, replace this with abfss:// container path.
base_out_path = r"file:///C:/tmp/bronze/inventory"
# base_out_path = "file:///tmp/bronze/inventory"
# base_out_path = "abfss://bronze@<storage-account>.dfs.core.windows.net/inventory"

output_format = "parquet"   # recommended: parquet. Use "csv" only for basic demo visibility.

# ---------------------------------------------------------
# 2) Small helper functions
# ---------------------------------------------------------
def read_mysql_table(table_or_query: str):
    return spark.read.jdbc(url=mysql_url, table=table_or_query, properties=mysql_props)


def execute_mysql_update(sql: str):
    """Execute UPDATE/INSERT/DDL statement in MySQL using JVM JDBC."""
    jvm = spark._sc._gateway.jvm
    driver_manager = jvm.java.sql.DriverManager
    conn = driver_manager.getConnection(mysql_url, mysql_props["user"], mysql_props["password"])
    stmt = conn.createStatement()
    try:
        stmt.executeUpdate(sql)
    finally:
        stmt.close()
        conn.close()


def write_incremental_data(df, table_name: str):
    table_out_path = f"{base_out_path}/{table_name}"

    if output_format.lower() == "csv":
        (df.coalesce(1)
           .write
           .mode("append")
           .option("header", "true")
           .csv(table_out_path))
    else:
        (df.write
           .mode("append")
           .parquet(table_out_path))


# ---------------------------------------------------------
# 3) Read active CDC metadata rows
# ---------------------------------------------------------
tracking_df = (read_mysql_table("cdc_tracking")
               .filter("is_active = 1")
               .select("table_name", "primary_key_col", "watermark_col", "last_watermark"))

tracking_rows = tracking_df.collect()

print(f"Active CDC tables found: {len(tracking_rows)}")

# ---------------------------------------------------------
# 4) Process each source table one by one
# ---------------------------------------------------------
for row in tracking_rows:
    table_name = row["table_name"]
    primary_key_col = row["primary_key_col"]
    watermark_col = row["watermark_col"]
    last_watermark = str(row["last_watermark"])

    print("=" * 80)
    print(f"Processing table       : {table_name}")
    print(f"Primary key column     : {primary_key_col}")
    print(f"Watermark column       : {watermark_col}")
    print(f"Last processed bookmark: {last_watermark}")

    # Query-based CDC extraction.
    # Alias is mandatory for JDBC subquery.
    inc_query = f"""
    (
      SELECT *
      FROM {table_name}
      WHERE {watermark_col} > '{last_watermark}'
    ) AS src
    """

    inc_df = read_mysql_table(inc_query)

    # Add ingestion metadata columns for bronze/audit understanding.
    inc_df = (inc_df
              .withColumn("cdc_source_table", lit(table_name))
              .withColumn("cdc_primary_key_col", lit(primary_key_col))
              .withColumn("cdc_watermark_col", lit(watermark_col))
              .withColumn("cdc_extracted_at", current_timestamp()))

    inc_df.cache()
    row_count = inc_df.count()
    print(f"Changed rows extracted : {row_count}")

    if row_count == 0:
        print(f"No changes found for {table_name}. Bookmark not updated.")
        inc_df.unpersist()
        continue

    inc_df.show(20, truncate=False)

    # Write changed rows to table-specific bronze folder.
    write_incremental_data(inc_df, table_name)
    print(f"Data written to        : {base_out_path}/{table_name}")

    # Get max watermark from extracted rows.
    max_watermark = (inc_df
                     .agg(spark_max(col(watermark_col)).cast("string").alias("max_watermark"))
                     .collect()[0]["max_watermark"])

    print(f"New max bookmark       : {max_watermark}")

    # Update bookmark only after successful write.
    update_sql = f"""
    UPDATE cdc_tracking
    SET last_watermark = '{max_watermark}'
    WHERE table_name = '{table_name}'
    """
    execute_mysql_update(update_sql)

    print(f"Bookmark updated for   : {table_name}")
    inc_df.unpersist()

print("=" * 80)
print("Multi-table CDC run completed.")
