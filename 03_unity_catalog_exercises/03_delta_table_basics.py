# ============================================================
# SIMPLE DELTA DEMO (ADLS Path Version — Exercise-wise Script)
# Demonstrates: CREATE + INSERT + UPDATE + DELETE + MERGE
# And shows changes at the table LOCATION (data files + _delta_log).
#
# IMPORTANT:
# - Same operations as before: DROP, CREATE (LOCATION), INSERT, UPDATE, DELETE, MERGE, HISTORY, Time Travel
# - Use an ADLS abfss:// path as the LOCATION.
# - Run in a DEV catalog/schema (recommended).
# ============================================================

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ----------------------------
# EXERCISE 0) Set UC context + choose ADLS location
# ----------------------------
CATALOG = "ecom_dev"
SCHEMA  = "delta_lab"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# ADLS Delta folder (table will be created at this LOCATION)
# Replace with YOUR external location governed path (recommended).
# Example:
# abfss://<container>@<account>.dfs.core.windows.net/<base>/simple_delta_demo/orders
DELTA_LOCATION = "abfss://data@uclabdemo.dfs.core.windows.net/uc_lab/simple_delta_demo/orders"

# Use a unique table name to avoid collisions, but keep the LOCATION stable for file observation
TABLE_NAME = "demo_orders"

print("Using:", f"{CATALOG}.{SCHEMA}")
print("TABLE_NAME:", TABLE_NAME)
print("DELTA_LOCATION:", DELTA_LOCATION)

# STOP:
# - In Azure Portal, open the container and navigate to:
#   uc_lab/simple_delta_demo/orders
# - Keep this tab open to observe _delta_log and parquet files after each step.


# ============================================================
# EXERCISE 1) Clean start (metastore only)
# Goal:
# - DROP TABLE if exists (removes metadata)
# Note:
# - Since LOCATION is external, dropping the table typically does NOT delete ADLS files.
#   If you want a clean folder, delete it manually from Azure Portal before Exercise 2.
# ============================================================

print("\n[EXERCISE 1] DROP TABLE (metadata cleanup)\n")
spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")

print("If you want a clean ADLS folder, delete the folder manually in Azure Portal now:")
print("Path:", DELTA_LOCATION)

# STOP:
# - In Azure Portal: optionally delete the folder to start from empty.
# - Then continue.


# ============================================================
# EXERCISE 2) Create Delta table at a specific ADLS LOCATION
# Goal:
# - Create table pointing to the ADLS folder
# - After this, _delta_log should appear in that folder
# ============================================================

print("\n[EXERCISE 2] CREATE TABLE USING DELTA LOCATION\n")

spark.sql(f"""
CREATE TABLE {TABLE_NAME} (
  order_id INT,
  customer STRING,
  amount INT,
  status STRING
)
USING DELTA
LOCATION '{DELTA_LOCATION}'
""")

print("Created table. Check Azure Portal now:")
print("- Look for _delta_log folder under the LOCATION")

# STOP (Azure Portal):
# - Confirm _delta_log exists.
# - Explain: Delta table = files + _delta_log transaction log.


# ============================================================
# EXERCISE 3) INSERT (one SQL statement = one transaction/commit)
# Goal:
# - Insert 3 rows
# - Observe: new parquet file(s) + new JSON in _delta_log
# ============================================================

print("\n[EXERCISE 3] INSERT rows (ONE transaction)\n")

spark.sql(f"""
INSERT INTO {TABLE_NAME} VALUES
(1, 'A', 100, 'CREATED'),
(2, 'B', 200, 'CREATED'),
(3, 'C', 300, 'CREATED')
""")

spark.sql(f"SELECT * FROM {TABLE_NAME} ORDER BY order_id").show(truncate=False)

print("Azure Portal check:")
print("- New parquet data file(s) should appear")
print("- _delta_log should have a new JSON commit file")

# STOP:
# - Teach: each statement => one commit/version.


# ============================================================
# EXERCISE 4) UPDATE (copy-on-write)
# Goal:
# - Update one row
# - Observe: new commit, more files (often), old files remain physically
# ============================================================

print("\n[EXERCISE 4] UPDATE one row (ONE transaction)\n")

spark.sql(f"""
UPDATE {TABLE_NAME}
SET status = 'SHIPPED'
WHERE order_id = 2
""")

spark.sql(f"SELECT * FROM {TABLE_NAME} ORDER BY order_id").show(truncate=False)

print("Azure Portal check:")
print("- _delta_log has a new commit JSON")
print("- You may see additional parquet files (copy-on-write)")

# STOP:
# - Teach: Delta does NOT edit parquet in place.


# ============================================================
# EXERCISE 5) DELETE (logical delete)
# Goal:
# - Delete one row
# - Observe: new commit; physical cleanup happens later via VACUUM
# ============================================================

print("\n[EXERCISE 5] DELETE one row (logical delete)\n")

spark.sql(f"DELETE FROM {TABLE_NAME} WHERE order_id = 1")

spark.sql(f"SELECT * FROM {TABLE_NAME} ORDER BY order_id").show(truncate=False)

print("Azure Portal check:")
print("- _delta_log has a new commit JSON")
print("- Old parquet files may still exist physically (not removed yet)")

# STOP:
# - Teach: deletes are logical at snapshot level; physical deletion is deferred.


# ============================================================
# EXERCISE 6) MERGE (Upsert) in ONE transaction
# Goal:
# - Update existing (order_id=2) + insert new (order_id=4)
# ============================================================

print("\n[EXERCISE 6] MERGE (upsert) in ONE transaction\n")

spark.sql(f"""
MERGE INTO {TABLE_NAME} t
USING (
  SELECT 2 AS order_id, 'B' AS customer, 250 AS amount, 'DELIVERED' AS status
  UNION ALL
  SELECT 4 AS order_id, 'D' AS customer, 400 AS amount, 'CREATED' AS status
) s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")

spark.sql(f"SELECT * FROM {TABLE_NAME} ORDER BY order_id").show(truncate=False)

print("Azure Portal check:")
print("- One new commit JSON for the MERGE")
print("- New parquet files may appear")

# STOP:
# - Teach: MERGE is the most common CDC/upsert pattern in Delta.


# ============================================================
# EXERCISE 7) HISTORY + Time Travel
# Goal:
# - View versions/operations
# - Read an older version (example: VERSION AS OF 1)
# ============================================================

print("\n[EXERCISE 7] DESCRIBE HISTORY\n")
spark.sql(f"DESCRIBE HISTORY {TABLE_NAME}").show(truncate=False)

print("\n[Time Travel] Read older version (example: VERSION AS OF 1)\n")
try:
    spark.sql(f"SELECT * FROM {TABLE_NAME} VERSION AS OF 1 ORDER BY order_id").show(truncate=False)
except Exception as e:
    print("Time travel query failed (maybe version 1 doesn't exist in your run). Error:", e)

# STOP:
# - Teach: Time travel relies on retained files. VACUUM reduces this window.


# ============================================================
# EXERCISE 8) OPTIONAL: VACUUM (physical deletion demo)
# WARNING:
# - Do NOT use RETAIN 0 HOURS in production.
# - Many workspaces block this due to retention safety checks.
# ============================================================

# Uncomment only if you want to demonstrate physical deletion:
#
# print("\n[EXERCISE 8 - OPTIONAL] VACUUM (demo only!)\n")
# spark.sql(f"VACUUM {TABLE_NAME} RETAIN 0 HOURS")
# print("Azure Portal check:")
# print("- Some old parquet files may be physically removed")
# print("- Time travel to old versions may fail after aggressive vacuum")

print("\nDemo ready for ADLS + Azure Portal visualization.")
print("Key observation:")
print("- Each SQL statement creates a new commit in _delta_log (one transaction).")
print("- UPDATE/DELETE/MERGE do NOT edit files in place: they add new files and logically remove old ones via the log.")
