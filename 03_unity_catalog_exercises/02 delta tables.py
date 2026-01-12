# =============================================================================
# Databricks / Delta Lake — Complete Hands-On Guide (Single Script)
# =============================================================================
# Goal:
# - Learn Delta Tables by doing: create, write, read, schema enforcement/evolution,
#   UPDATE/DELETE, MERGE (upsert), Time Travel, RESTORE, OPTIMIZE, VACUUM,
#   partitioning, constraints, and some realistic exercises.
#
# Audience:
# - You already know UC basics (catalog/schema, storage credentials, external locations).
#
# IMPORTANT NOTES:
# 1) "Delta" is a STORAGE/TABLE FORMAT (ACID + _delta_log). It is not the same as
#    "managed vs external". Managed/external is ownership & lifecycle.
# 2) This script is designed for a 60–90 minute guided session.
# 3) Run this in a DEV catalog/schema only.
# 4) Some commands (OPTIMIZE/ZORDER) may depend on your Databricks edition/runtime.
#
# =============================================================================
# SECTION 0 — Set your environment (Catalog/Schema + Paths)
# =============================================================================

from pyspark.sql import functions as F
import time, uuid

# ---- Choose where your tables live (Unity Catalog) ----
CATALOG = "ecom_dev"     # change if needed
SCHEMA  = "delta_lab"    # safe dev schema

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# Unique suffix so you can re-run without collisions
suffix = f"{int(time.time())}_{uuid.uuid4().hex[:6]}"

# Table names
T_BRONZE_CSV_TABLE   = f"orders_csv_bronze_{suffix}"     # non-delta table on CSV (optional)
T_BRONZE_DELTA       = f"orders_bronze_delta_{suffix}"   # delta table (bronze)
T_SILVER_DELTA       = f"orders_silver_delta_{suffix}"   # delta table (silver)
T_CUSTOMERS_DELTA    = f"customers_delta_{suffix}"       # for merge demo
T_MERGE_TARGET       = f"customers_target_{suffix}"      # merge target

# ---- Choose a working folder for files ----
# Preferred in UC: use a Volume path like:
#   /Volumes/<catalog>/<schema>/<volume>/...
#
# If you still want to use DBFS for a quick lab, you can use:
#   dbfs:/FileStore/...
#
# I will default to DBFS to match your question; you can replace with Volume later.
BASE_FILE_PATH = f"dbfs:/FileStore/delta_lab/{suffix}"

CSV_PATH       = f"{BASE_FILE_PATH}/raw/orders.csv"
DELTA_PATH     = f"{BASE_FILE_PATH}/delta/orders_delta_path"   # path-based delta table demo

print("Using UC:", f"{CATALOG}.{SCHEMA}")
print("Base file path:", BASE_FILE_PATH)


# =============================================================================
# SECTION 1 — Create sample data (realistic orders)
# =============================================================================
# We'll create a DataFrame and also write it as CSV to simulate "landing files".

orders = [
    ("O-1001","C-01","P-01","2026-01-01","DELIVERED", 1, 499.0),
    ("O-1002","C-02","P-02","2026-01-01","CANCELLED", 2, 299.0),
    ("O-1003","C-01","P-03","2026-01-02","PLACED",    1, 999.0),
    ("O-1004","C-03","P-02","2026-01-03","SHIPPED",   3, 299.0),
    ("O-1005","C-04","P-01","2026-01-04","DELIVERED", 2, 499.0),
    ("O-1006","C-02","P-03","2026-01-05","RETURNED",  1, 999.0),
    ("O-1007","C-05","P-04","2026-01-06","PLACED",    5, 199.0),
    ("O-1008","C-03","P-05","2026-01-07","DELIVERED", 1, 1499.0),
]
cols = ["order_id","customer_id","product_id","order_date","order_status","quantity","price"]

df_orders = (
    spark.createDataFrame(orders, cols)
    .withColumn("order_date", F.to_date("order_date"))
    .withColumn("ingest_ts", F.current_timestamp())
    .withColumn("amount", F.col("quantity") * F.col("price"))
)

df_orders.show(truncate=False)
df_orders.printSchema()


# =============================================================================
# SECTION 2 — Write the data as CSV (simulate raw landed files)
# =============================================================================
# CSV has no ACID, no schema enforcement, no time travel.
# We'll use it to demonstrate how Delta improves reliability.

(
    df_orders.coalesce(1)  # for lab simplicity, create one file
    .write.mode("overwrite")
    .option("header", True)
    .csv(CSV_PATH)
)
print("CSV written to:", CSV_PATH)


# =============================================================================
# SECTION 3 — Create a "CSV table" (NOT Delta) and query it
# =============================================================================
# This is optional but helps you see:
# - You can query CSV as a table
# - But it is not Delta (no _delta_log, no MERGE/UPDATE/DELETE reliably)

spark.sql(f"DROP TABLE IF EXISTS {T_BRONZE_CSV_TABLE}")

spark.sql(f"""
CREATE TABLE {T_BRONZE_CSV_TABLE}
USING CSV
OPTIONS (
  path '{CSV_PATH}',
  header 'true',
  inferSchema 'true'
)
""")

spark.sql(f"SELECT * FROM {T_BRONZE_CSV_TABLE} ORDER BY order_id").show(truncate=False)

# Check provider & location
spark.sql(f"DESCRIBE EXTENDED {T_BRONZE_CSV_TABLE}").show(200, truncate=False)


# =============================================================================
# SECTION 4 — Create a Delta table from CSV (Bronze Delta)
# =============================================================================
# Delta tables = files + _delta_log transaction log.
# We'll do CTAS (Create Table As Select) to convert CSV data into Delta.

spark.sql(f"DROP TABLE IF EXISTS {T_BRONZE_DELTA}")

spark.sql(f"""
CREATE TABLE {T_BRONZE_DELTA}
USING DELTA
AS
SELECT
  order_id,
  customer_id,
  product_id,
  CAST(order_date AS DATE) AS order_date,
  order_status,
  CAST(quantity AS INT) AS quantity,
  CAST(price AS DOUBLE) AS price,
  (CAST(quantity AS INT) * CAST(price AS DOUBLE)) AS amount,
  current_timestamp() AS ingest_ts
FROM {T_BRONZE_CSV_TABLE}
""")

spark.sql(f"SELECT * FROM {T_BRONZE_DELTA} ORDER BY order_id").show(truncate=False)

# Delta metadata check
spark.sql(f"DESCRIBE DETAIL {T_BRONZE_DELTA}").show(truncate=False)
spark.sql(f"DESCRIBE HISTORY {T_BRONZE_DELTA}").show(truncate=False)


# =============================================================================
# SECTION 5 — Delta Schema Enforcement (Why Delta is safer)
# =============================================================================
# Delta enforces schema consistency.
# Example: try writing a wrong type or missing columns in a way that violates schema.
#
# We'll create a "bad" DataFrame where quantity is a string.
df_bad = (
    df_orders
    .withColumn("quantity", F.lit("BAD_QTY"))  # wrong type
)

# Attempt to append -> should fail or require explicit handling
# (In many runtimes, this will throw an exception due to schema mismatch.)
try:
    df_bad.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.{T_BRONZE_DELTA}")
except Exception as e:
    print("\nExpected failure (schema enforcement):", str(e)[:250], "...\n")


# =============================================================================
# SECTION 6 — Delta Schema Evolution (Controlled change)
# =============================================================================
# You CAN evolve schema if you decide to allow it.
# Example: add a new column "source_system".

df_newcol = df_orders.withColumn("source_system", F.lit("csv_landing"))

# Append with mergeSchema (DataFrame writer)
df_newcol.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(T_BRONZE_DELTA)

spark.sql(f"SELECT order_id, source_system FROM {T_BRONZE_DELTA} ORDER BY order_id LIMIT 5").show(truncate=False)
spark.sql(f"DESCRIBE {T_BRONZE_DELTA}").show(truncate=False)
spark.sql(f"DESCRIBE HISTORY {T_BRONZE_DELTA}").show(truncate=False)


# =============================================================================
# SECTION 7 — UPDATE / DELETE (Delta supports DML)
# =============================================================================
# CSV/Parquet tables typically don't reliably support these transactional operations.
# Delta does.

# Update: mark one order as DELIVERED
spark.sql(f"""
UPDATE {T_BRONZE_DELTA}
SET order_status = 'DELIVERED'
WHERE order_id = 'O-1003'
""")

# Delete: remove cancelled orders
spark.sql(f"DELETE FROM {T_BRONZE_DELTA} WHERE order_status = 'CANCELLED'")

spark.sql(f"SELECT order_id, order_status FROM {T_BRONZE_DELTA} ORDER BY order_id").show(truncate=False)
spark.sql(f"DESCRIBE HISTORY {T_BRONZE_DELTA}").show(truncate=False)


# =============================================================================
# SECTION 8 — Time Travel (Query previous versions)
# =============================================================================
# Delta tracks versions of the table. You can read older snapshots.

hist = spark.sql(f"DESCRIBE HISTORY {T_BRONZE_DELTA}")
hist.show(truncate=False)

# Capture a version number from history (lowest is usually the earliest)
versions = [row["version"] for row in hist.select("version").collect()]
min_version = min(versions)
max_version = max(versions)

print("Earliest version:", min_version, "Latest version:", max_version)

# Query earliest version (before update/delete/append happened)
spark.sql(f"""
SELECT order_id, order_status, quantity, price
FROM {T_BRONZE_DELTA} VERSION AS OF {min_version}
ORDER BY order_id
""").show(truncate=False)

# Query latest version
spark.sql(f"""
SELECT order_id, order_status, quantity, price
FROM {T_BRONZE_DELTA} VERSION AS OF {max_version}
ORDER BY order_id
""").show(truncate=False)


# =============================================================================
# SECTION 9 — RESTORE (Rollback table to an older version)
# =============================================================================
# If a bad pipeline run happens, RESTORE can quickly revert the table.

# Choose a safe restore target: earliest version
spark.sql(f"RESTORE TABLE {T_BRONZE_DELTA} TO VERSION AS OF {min_version}")
spark.sql(f"SELECT order_id, order_status FROM {T_BRONZE_DELTA} ORDER BY order_id").show(truncate=False)
spark.sql(f"DESCRIBE HISTORY {T_BRONZE_DELTA}").show(truncate=False)


# =============================================================================
# SECTION 10 — MERGE (Upsert / CDC pattern) — the #1 production use case
# =============================================================================
# We'll build a Customers table and do CDC-like updates.

customers_initial = [
    ("C-01","Riya","Sharma","riya@example.com","Pune",  True, "2026-01-01 10:00:00"),
    ("C-02","Aman","Verma","aman@example.com","Mumbai", True, "2026-01-01 11:00:00"),
    ("C-03","Neha","Singh","neha@example.com","Delhi",  True, "2026-01-01 12:00:00"),
]
cust_cols = ["customer_id","first_name","last_name","email","city","is_active","last_modified_ts"]

df_cust_init = (
    spark.createDataFrame(customers_initial, cust_cols)
    .withColumn("last_modified_ts", F.to_timestamp("last_modified_ts"))
)

spark.sql(f"DROP TABLE IF EXISTS {T_MERGE_TARGET}")
df_cust_init.write.format("delta").mode("overwrite").saveAsTable(T_MERGE_TARGET)

print("Initial customers:")
spark.sql(f"SELECT * FROM {T_MERGE_TARGET} ORDER BY customer_id").show(truncate=False)

# Now create CDC batch (updates + inserts + soft delete)
customers_cdc = [
    ("C-02","Aman","Verma","aman_new@example.com","Mumbai", True,  "2026-01-02 09:00:00"),  # email updated
    ("C-03","Neha","Singh","neha@example.com","Bengaluru", True,  "2026-01-02 09:05:00"),  # city updated
    ("C-04","Kabir","Joshi","kabir@example.com","Pune",    True,  "2026-01-02 09:10:00"),  # new insert
    ("C-01","Riya","Sharma","riya@example.com","Pune",     False, "2026-01-02 09:20:00"),  # soft delete / inactive
]
df_cdc = (
    spark.createDataFrame(customers_cdc, cust_cols)
    .withColumn("last_modified_ts", F.to_timestamp("last_modified_ts"))
)

df_cdc.createOrReplaceTempView("cust_cdc")

# MERGE logic:
# - If matched and CDC record is newer: update
# - If not matched: insert
spark.sql(f"""
MERGE INTO {T_MERGE_TARGET} AS t
USING cust_cdc AS s
ON t.customer_id = s.customer_id
WHEN MATCHED AND s.last_modified_ts > t.last_modified_ts THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

print("After MERGE:")
spark.sql(f"SELECT * FROM {T_MERGE_TARGET} ORDER BY customer_id").show(truncate=False)

spark.sql(f"DESCRIBE HISTORY {T_MERGE_TARGET}").show(truncate=False)


# =============================================================================
# SECTION 11 — Partitioning (when & how)
# =============================================================================
# Partitioning helps when you frequently filter by a column with good cardinality
# (not too high, not too low) and large data.
# We'll create a Silver table partitioned by order_date for demonstration.

spark.sql(f"DROP TABLE IF EXISTS {T_SILVER_DELTA}")

spark.sql(f"""
CREATE TABLE {T_SILVER_DELTA} (
  order_id STRING,
  customer_id STRING,
  product_id STRING,
  order_date DATE,
  order_status STRING,
  quantity INT,
  price DOUBLE,
  amount DOUBLE,
  source_system STRING,
  ingest_ts TIMESTAMP
)
USING DELTA
PARTITIONED BY (order_date)
""")

# Insert from bronze
spark.sql(f"""
INSERT INTO {T_SILVER_DELTA}
SELECT
  order_id, customer_id, product_id, order_date, order_status,
  quantity, price, amount,
  COALESCE(source_system, 'unknown') AS source_system,
  ingest_ts
FROM {T_BRONZE_DELTA}
""")

spark.sql(f"SELECT order_date, COUNT(*) cnt FROM {T_SILVER_DELTA} GROUP BY order_date ORDER BY order_date").show()


# =============================================================================
# SECTION 12 — OPTIMIZE / ZORDER (performance tuning, may depend on runtime)
# =============================================================================
# OPTIMIZE compacts small files. ZORDER helps skipping data for selective queries.
# Note: Availability depends on your Databricks edition/runtime.

try:
    spark.sql(f"OPTIMIZE {T_SILVER_DELTA}")
    spark.sql(f"OPTIMIZE {T_SILVER_DELTA} ZORDER BY (customer_id)")
    print("OPTIMIZE/ZORDER executed (if supported).")
except Exception as e:
    print("\nOPTIMIZE/ZORDER not available or not permitted in this environment:", str(e)[:200], "...\n")


# =============================================================================
# SECTION 13 — Constraints (basic quality checks)
# =============================================================================
# Delta supports constraints like NOT NULL and CHECK (varies by environment).
# We'll show a safe example.

T_CONSTRAINTS = f"orders_constraints_{suffix}"
spark.sql(f"DROP TABLE IF EXISTS {T_CONSTRAINTS}")

spark.sql(f"""
CREATE TABLE {T_CONSTRAINTS} (
  order_id STRING NOT NULL,
  quantity INT,
  price DOUBLE,
  amount DOUBLE,
  CONSTRAINT positive_qty CHECK (quantity > 0),
  CONSTRAINT positive_price CHECK (price > 0)
)
USING DELTA
""")

# Insert valid rows
spark.sql(f"""
INSERT INTO {T_CONSTRAINTS}
SELECT order_id, quantity, price, amount
FROM {T_SILVER_DELTA}
LIMIT 3
""")

spark.sql(f"SELECT * FROM {T_CONSTRAINTS}").show(truncate=False)

# Try inserting invalid row (should fail)
try:
    spark.sql(f"INSERT INTO {T_CONSTRAINTS} VALUES ('O-BAD', -1, 10.0, -10.0)")
except Exception as e:
    print("\nExpected failure (constraint violation):", str(e)[:250], "...\n")


# =============================================================================
# SECTION 14 — Path-based Delta table (Delta can be table OR just a Delta folder)
# =============================================================================
# Delta is fundamentally a folder with data files + _delta_log.
# You can write to a path and read it without registering as a table.

# Write to delta path
df_orders.write.format("delta").mode("overwrite").save(DELTA_PATH)
print("Delta folder written to:", DELTA_PATH)

# Read from delta path
spark.read.format("delta").load(DELTA_PATH).show(truncate=False)

# Register that path as an external table (optional)
T_PATH_TABLE = f"orders_delta_path_table_{suffix}"
spark.sql(f"DROP TABLE IF EXISTS {T_PATH_TABLE}")
spark.sql(f"CREATE TABLE {T_PATH_TABLE} USING DELTA LOCATION '{DELTA_PATH}'")
spark.sql(f"SELECT COUNT(*) AS c FROM {T_PATH_TABLE}").show()


# =============================================================================
# SECTION 15 — VACUUM (garbage collection) + Retention warning
# =============================================================================
# VACUUM removes old files no longer needed by Delta.
# Time travel depends on file retention. If you vacuum aggressively, you lose old versions.
#
# DO NOT run with very low retention in production.
#
# We'll just show the command and catch if restricted.

try:
    # Default vacuum respects retention (often 7 days). In labs, you might not see changes.
    spark.sql(f"VACUUM {T_SILVER_DELTA}")
    print("VACUUM executed (default retention).")
except Exception as e:
    print("\nVACUUM not available or restricted:", str(e)[:200], "...\n")


# =============================================================================
# SECTION 16 — Hands-on Exercises (Do these yourself)
# =============================================================================
# EXERCISE 1 (Schema Evolution)
# - Add a new column "discount" to the bronze delta table by appending new data.
# - Use mergeSchema option.
#
# EXERCISE 2 (Time Travel Investigation)
# - Run 2 updates on the silver table (change order_status).
# - Use DESCRIBE HISTORY and query VERSION AS OF for before/after.
#
# EXERCISE 3 (CDC MERGE)
# - Extend the customer CDC to include:
#   - C-04 city update
#   - a new customer C-06
# - Merge into the target and validate results.
#
# EXERCISE 4 (Delete + Restore)
# - DELETE all rows for one customer_id in silver.
# - RESTORE the table to the version before delete.
#
# EXERCISE 5 (Quality Constraints)
# - Create a table with CHECK(amount = quantity * price)
# - Try inserting invalid rows and observe failures.
#
# EXERCISE 6 (Partition Strategy)
# - Create a new table partitioned by order_status and compare query behavior
#   (in real large data, partition choice matters).
#
# =============================================================================
# CLEANUP (Optional)
# =============================================================================
# If you want to clean tables:
# spark.sql(f"DROP TABLE IF EXISTS {T_BRONZE_CSV_TABLE}")
# spark.sql(f"DROP TABLE IF EXISTS {T_BRONZE_DELTA}")
# spark.sql(f"DROP TABLE IF EXISTS {T_SILVER_DELTA}")
# spark.sql(f"DROP TABLE IF EXISTS {T_MERGE_TARGET}")
# spark.sql(f"DROP TABLE IF EXISTS {T_CONSTRAINTS}")
# spark.sql(f"DROP TABLE IF EXISTS {T_PATH_TABLE}")
#
# For files (DBFS):
# dbutils.fs.rm(BASE_FILE_PATH, True)
#
print("\nDelta Lake hands-on lab finished.\n")
