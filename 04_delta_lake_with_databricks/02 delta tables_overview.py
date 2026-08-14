# =============================================================================
# Databricks / Delta Lake — Complete Hands-On Guide (Step-by-Step Exercises)
# =============================================================================
# Goal:
# - Learn Delta Tables by doing: create, write, read, schema enforcement/evolution,
#   UPDATE/DELETE, MERGE (upsert), Time Travel, RESTORE, OPTIMIZE, VACUUM,
#   partitioning, constraints, and realistic exercises.
#
# IMPORTANT:
# - Same operations as your original script (no logic removed).
# - Re-structured into EXERCISES with STOP points for teaching.
# - Filesystem interactions: verify graphically in UI/Azure/DBFS browser as you prefer.
#
# Recommended style:
# - Run each exercise cell/block separately.
# - After each block, show the result in:
#   1) Data Explorer (tables + history)
#   2) DBFS browser / Volumes browser / Azure Portal (folders/files)
# =============================================================================

from pyspark.sql import functions as F
import time, uuid

# =============================================================================
# EXERCISE 0 — Set environment (Catalog/Schema + Paths)
# Goal:
# - Create/use dev catalog & schema
# - Define unique names to avoid collisions
# =============================================================================

CATALOG = "ecom_dev"     # change if needed
SCHEMA  = "delta_lab"    # safe dev schema

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

suffix = f"{int(time.time())}_{uuid.uuid4().hex[:6]}"

T_BRONZE_CSV_TABLE   = f"orders_csv_bronze_{suffix}"
T_BRONZE_DELTA       = f"orders_bronze_delta_{suffix}"
T_SILVER_DELTA       = f"orders_silver_delta_{suffix}"
T_CUSTOMERS_DELTA    = f"customers_delta_{suffix}"       # (kept, even if unused)
T_MERGE_TARGET       = f"customers_target_{suffix}"

BASE_FILE_PATH = f"dbfs:/FileStore/delta_lab/{suffix}"
CSV_PATH       = f"{BASE_FILE_PATH}/raw/orders.csv"
DELTA_PATH     = f"{BASE_FILE_PATH}/delta/orders_delta_path"

print("Using UC:", f"{CATALOG}.{SCHEMA}")
print("Tables:", T_BRONZE_CSV_TABLE, T_BRONZE_DELTA, T_SILVER_DELTA, T_MERGE_TARGET, sep="\n - ")
print("Base file path:", BASE_FILE_PATH)
print("CSV path:", CSV_PATH)
print("Delta path:", DELTA_PATH)

# STOP:
# - Show students catalog/schema in Data Explorer.
# - Show BASE_FILE_PATH location in DBFS browser (or explain it).


# =============================================================================
# EXERCISE 1 — Create sample data (realistic orders)
# Goal:
# - Create df_orders with derived columns (ingest_ts, amount)
# =============================================================================

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

# STOP:
# - Ask students: why keep ingest_ts? why compute amount?


# =============================================================================
# EXERCISE 2 — Write data as CSV (simulate raw landed files)
# Goal:
# - Create "landing files" on storage
# =============================================================================

(
    df_orders.coalesce(1)
    .write.mode("overwrite")
    .option("header", True)
    .csv(CSV_PATH)
)
print("CSV written to:", CSV_PATH)

# STOP (filesystem demo):
# - Show the folder in DBFS browser:
#   /FileStore/delta_lab/<suffix>/raw/orders.csv/
# - Explain: CSV is just files, no ACID log.


# =============================================================================
# EXERCISE 3 — Create a CSV table (NOT Delta) and query it
# Goal:
# - Register CSV files as a table and query
# - Inspect metadata (provider/location)
# =============================================================================

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
spark.sql(f"DESCRIBE EXTENDED {T_BRONZE_CSV_TABLE}").show(200, truncate=False)

# STOP:
# - Show in Data Explorer: provider CSV, location points to your CSV folder.
# - Ask: What is missing compared to Delta? (No _delta_log, no history, no ACID)


# =============================================================================
# EXERCISE 4 — Convert CSV table to Delta (Bronze Delta) using CTAS
# Goal:
# - Create a Delta table from CSV data
# - Inspect Delta detail + history
# =============================================================================

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
spark.sql(f"DESCRIBE DETAIL {T_BRONZE_DELTA}").show(truncate=False)
spark.sql(f"DESCRIBE HISTORY {T_BRONZE_DELTA}").show(truncate=False)

# STOP:
# - Show in storage: Delta table folder contains _delta_log.
# - Show in Data Explorer: History exists now.


# =============================================================================
# EXERCISE 5 — Delta Schema Enforcement (expected failure)
# Goal:
# - Attempt to append wrong schema/type to show enforcement
# =============================================================================

df_bad = df_orders.withColumn("quantity", F.lit("BAD_QTY"))

try:
    df_bad.write.format("delta").mode("append").saveAsTable(f"{CATALOG}.{SCHEMA}.{T_BRONZE_DELTA}")
except Exception as e:
    print("\nExpected failure (schema enforcement):", str(e)[:250], "...\n")

# STOP:
# - Read the error message with students.
# - Explain: Delta protects schema unless you explicitly allow evolution.


# =============================================================================
# EXERCISE 6 — Delta Schema Evolution (controlled change)
# Goal:
# - Add new column using mergeSchema and append
# =============================================================================

df_newcol = df_orders.withColumn("source_system", F.lit("csv_landing"))

df_newcol.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(T_BRONZE_DELTA)

spark.sql(f"SELECT order_id, source_system FROM {T_BRONZE_DELTA} ORDER BY order_id LIMIT 5").show(truncate=False)
spark.sql(f"DESCRIBE {T_BRONZE_DELTA}").show(truncate=False)
spark.sql(f"DESCRIBE HISTORY {T_BRONZE_DELTA}").show(truncate=False)

# STOP:
# - Show the new column in schema.
# - Highlight that history version increased.


# =============================================================================
# EXERCISE 7 — UPDATE / DELETE (Delta supports DML)
# Goal:
# - Run UPDATE and DELETE, then check table + history
# =============================================================================

spark.sql(f"""
UPDATE {T_BRONZE_DELTA}
SET order_status = 'DELIVERED'
WHERE order_id = 'O-1003'
""")

spark.sql(f"DELETE FROM {T_BRONZE_DELTA} WHERE order_status = 'CANCELLED'")

spark.sql(f"SELECT order_id, order_status FROM {T_BRONZE_DELTA} ORDER BY order_id").show(truncate=False)
spark.sql(f"DESCRIBE HISTORY {T_BRONZE_DELTA}").show(truncate=False)

# STOP:
# - Ask students: Why is DML safe in Delta? (transaction log)


# =============================================================================
# EXERCISE 8 — Time Travel (query previous versions)
# Goal:
# - Use DESCRIBE HISTORY and VERSION AS OF
# =============================================================================

hist = spark.sql(f"DESCRIBE HISTORY {T_BRONZE_DELTA}")
hist.show(truncate=False)

versions = [row["version"] for row in hist.select("version").collect()]
min_version = min(versions)
max_version = max(versions)

print("Earliest version:", min_version, "Latest version:", max_version)

spark.sql(f"""
SELECT order_id, order_status, quantity, price
FROM {T_BRONZE_DELTA} VERSION AS OF {min_version}
ORDER BY order_id
""").show(truncate=False)

spark.sql(f"""
SELECT order_id, order_status, quantity, price
FROM {T_BRONZE_DELTA} VERSION AS OF {max_version}
ORDER BY order_id
""").show(truncate=False)

# STOP:
# - Explain: time travel works because old snapshots are retained (until VACUUM removes files).


# =============================================================================
# EXERCISE 9 — RESTORE (rollback table to an older version)
# Goal:
# - Restore to earliest version and check data + history
# =============================================================================

spark.sql(f"RESTORE TABLE {T_BRONZE_DELTA} TO VERSION AS OF {min_version}")
spark.sql(f"SELECT order_id, order_status FROM {T_BRONZE_DELTA} ORDER BY order_id").show(truncate=False)
spark.sql(f"DESCRIBE HISTORY {T_BRONZE_DELTA}").show(truncate=False)

# STOP:
# - Show that RESTORE creates a NEW history entry (it’s a new commit).


# =============================================================================
# EXERCISE 10 — MERGE (Upsert / CDC pattern)
# Goal:
# - Create target table, build CDC batch, MERGE into target
# =============================================================================

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

customers_cdc = [
    ("C-02","Aman","Verma","aman_new@example.com","Mumbai", True,  "2026-01-02 09:00:00"),
    ("C-03","Neha","Singh","neha@example.com","Bengaluru", True,  "2026-01-02 09:05:00"),
    ("C-04","Kabir","Joshi","kabir@example.com","Pune",    True,  "2026-01-02 09:10:00"),
    ("C-01","Riya","Sharma","riya@example.com","Pune",     False, "2026-01-02 09:20:00"),
]

df_cdc = (
    spark.createDataFrame(customers_cdc, cust_cols)
    .withColumn("last_modified_ts", F.to_timestamp("last_modified_ts"))
)

df_cdc.createOrReplaceTempView("cust_cdc")

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

# STOP:
# - Explain: MERGE = batch CDC; structured streaming can also do CDC patterns with MERGE.


# =============================================================================
# EXERCISE 11 — Partitioning (Silver table)
# Goal:
# - Create partitioned Delta table
# - Insert from bronze and validate distribution
# =============================================================================

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

# STOP (filesystem demo):
# - Show in storage: folders like order_date=2026-01-01 etc.
# - Explain partitioning trade-offs (pruning vs small files).


# =============================================================================
# EXERCISE 12 — OPTIMIZE / ZORDER (if supported)
# Goal:
# - Compact files and/or ZORDER for query skipping
# =============================================================================

try:
    spark.sql(f"OPTIMIZE {T_SILVER_DELTA}")
    spark.sql(f"OPTIMIZE {T_SILVER_DELTA} ZORDER BY (customer_id)")
    print("OPTIMIZE/ZORDER executed (if supported).")
except Exception as e:
    print("\nOPTIMIZE/ZORDER not available or not permitted:", str(e)[:200], "...\n")

# STOP:
# - If it runs: show history + file count changes (DESCRIBE DETAIL).
# - If it fails: explain edition/runtime permissions.


# =============================================================================
# EXERCISE 13 — Constraints (basic data quality)
# Goal:
# - Create a table with constraints
# - Insert valid rows
# - Try invalid insert and observe failure
# =============================================================================

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

spark.sql(f"""
INSERT INTO {T_CONSTRAINTS}
SELECT order_id, quantity, price, amount
FROM {T_SILVER_DELTA}
LIMIT 3
""")

spark.sql(f"SELECT * FROM {T_CONSTRAINTS}").show(truncate=False)

try:
    spark.sql(f"INSERT INTO {T_CONSTRAINTS} VALUES ('O-BAD', -1, 10.0, -10.0)")
except Exception as e:
    print("\nExpected failure (constraint violation):", str(e)[:250], "...\n")

# STOP:
# - Clarify: constraints are guardrails, not a replacement for full DQ frameworks.


# =============================================================================
# EXERCISE 14 — Path-based Delta (Delta folder without table registration)
# Goal:
# - Write Delta to a path
# - Read Delta from path
# - Register that path as a table (external-by-location behavior)
# =============================================================================

df_orders.write.format("delta").mode("overwrite").save(DELTA_PATH)
print("Delta folder written to:", DELTA_PATH)

spark.read.format("delta").load(DELTA_PATH).show(truncate=False)

T_PATH_TABLE = f"orders_delta_path_table_{suffix}"
spark.sql(f"DROP TABLE IF EXISTS {T_PATH_TABLE}")
spark.sql(f"CREATE TABLE {T_PATH_TABLE} USING DELTA LOCATION '{DELTA_PATH}'")
spark.sql(f"SELECT COUNT(*) AS c FROM {T_PATH_TABLE}").show()

# STOP (filesystem demo):
# - Show in DBFS: _delta_log exists at DELTA_PATH.
# - Explain: “Delta = folder + _delta_log” (table is just a registration over it).


# =============================================================================
# EXERCISE 15 — VACUUM (garbage collection) + retention warning
# Goal:
# - Show VACUUM behavior (may be restricted)
# =============================================================================

try:
    spark.sql(f"VACUUM {T_SILVER_DELTA}")
    print("VACUUM executed (default retention).")
except Exception as e:
    print("\nVACUUM not available or restricted:", str(e)[:200], "...\n")

# STOP:
# - Explain: Aggressive vacuum reduces time travel window and can break restores.


# =============================================================================
# EXERCISE 16 — Hands-on Exercises (Student tasks)
# =============================================================================
# EXERCISE 16.1 (Schema Evolution)
# - Add a new column "discount" to the bronze delta table by appending new data.
# - Use mergeSchema option.
#
# EXERCISE 16.2 (Time Travel Investigation)
# - Run 2 updates on the silver table (change order_status).
# - Use DESCRIBE HISTORY and query VERSION AS OF for before/after.
#
# EXERCISE 16.3 (CDC MERGE)
# - Extend the customer CDC to include:
#   - C-04 city update
#   - a new customer C-06
# - Merge into the target and validate results.
#
# EXERCISE 16.4 (Delete + Restore)
# - DELETE all rows for one customer_id in silver.
# - RESTORE the table to the version before delete.
#
# EXERCISE 16.5 (Quality Constraints)
# - Create a table with CHECK(amount = quantity * price)
# - Try inserting invalid rows and observe failures.
#
# EXERCISE 16.6 (Partition Strategy)
# - Create a new table partitioned by order_status and compare query behavior.
#
# =============================================================================
# CLEANUP (Optional)
# =============================================================================
# spark.sql(f"DROP TABLE IF EXISTS {T_BRONZE_CSV_TABLE}")
# spark.sql(f"DROP TABLE IF EXISTS {T_BRONZE_DELTA}")
# spark.sql(f"DROP TABLE IF EXISTS {T_SILVER_DELTA}")
# spark.sql(f"DROP TABLE IF EXISTS {T_MERGE_TARGET}")
# spark.sql(f"DROP TABLE IF EXISTS {T_CONSTRAINTS}")
# spark.sql(f"DROP TABLE IF EXISTS {T_PATH_TABLE}")
# dbutils.fs.rm(BASE_FILE_PATH, True)

print("\nDelta Lake hands-on lab prepared for exercise-wise execution.\n")
