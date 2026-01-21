# ============================================================
# SIMPLE DELTA DEMO (Single Script)
# Demonstrates: CREATE + INSERT + UPDATE + DELETE + MERGE
# And shows changes at the table LOCATION (data files + _delta_log).
#
# Run in: Databricks (recommended). Works best with dbutils.fs.ls().
# If dbutils is not available, it falls back to Python os.walk().
# ============================================================

from pyspark.sql import SparkSession
import shutil, os

spark = SparkSession.builder.getOrCreate()

# ----------------------------
# 0) Choose a simple table location (DBFS-style for demo)
# ----------------------------
# You can change this path. Keep it stable so you can observe files.
DBFS_PATH = "/dbfs/tmp/simple_delta_demo/orders"     # physical path on driver node
DELTA_LOCATION = "/dbfs/tmp/simple_delta_demo/orders"  # same string used in SQL LOCATION
TABLE_NAME = "demo_orders"

# ----------------------------
# Helper: list folder contents so you can "see" Delta anatomy
# ----------------------------
def list_path(title, p):
    print(f"\n--- {title}: {p} ---")
    # Databricks: prefer dbutils.fs.ls for DBFS paths
    try:
        # dbutils exists in Databricks notebooks
        display(dbutils.fs.ls(p))
        return
    except Exception:
        pass

    # Fallback: os.walk (works for /dbfs/... paths in Databricks driver)
    try:
        for root, dirs, files in os.walk(p):
            rel = root.replace(p, ".")
            print(rel)
            for d in sorted(dirs):
                print("  [DIR] ", d)
            for f in sorted(files):
                print("  [FILE]", f)
    except Exception as e:
        print("Could not list path:", e)

# ----------------------------
# 1) Clean start (metastore + physical files)
# ----------------------------
print("\n[STEP 1] Clean start: drop table + delete folder\n")

spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")

try:
    shutil.rmtree(DBFS_PATH)
except Exception:
    pass

list_path("After cleanup (folder may not exist yet)", "/dbfs/tmp/simple_delta_demo")

# ----------------------------
# 2) Create Delta table at a specific LOCATION
# ----------------------------
print("\n[STEP 2] Create Delta table at LOCATION\n")

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

# Observe: _delta_log should appear (may have version 0 commit)
list_path("Table folder after CREATE", DBFS_PATH)
list_path("_delta_log after CREATE", f"{DBFS_PATH}/_delta_log")

# ----------------------------
# 3) INSERT (one SQL statement = one transaction/commit)
# ----------------------------
print("\n[STEP 3] INSERT rows (ONE transaction)\n")

spark.sql(f"""
INSERT INTO {TABLE_NAME} VALUES
(1, 'A', 100, 'CREATED'),
(2, 'B', 200, 'CREATED'),
(3, 'C', 300, 'CREATED')
""")

spark.sql(f"SELECT * FROM {TABLE_NAME} ORDER BY order_id").show(truncate=False)

# Observe: new data files + new JSON commit in _delta_log
list_path("Table folder after INSERT (data files created)", DBFS_PATH)
list_path("_delta_log after INSERT (new commit JSON)", f"{DBFS_PATH}/_delta_log")

# ----------------------------
# 4) UPDATE (copy-on-write: add new files, remove old in log)
# ----------------------------
print("\n[STEP 4] UPDATE one row (still ONE transaction)\n")

spark.sql(f"""
UPDATE {TABLE_NAME}
SET status = 'SHIPPED'
WHERE order_id = 2
""")

spark.sql(f"SELECT * FROM {TABLE_NAME} ORDER BY order_id").show(truncate=False)

# Observe: another commit JSON; data files may increase (old not physically deleted yet)
list_path("Table folder after UPDATE", DBFS_PATH)
list_path("_delta_log after UPDATE", f"{DBFS_PATH}/_delta_log")

# ----------------------------
# 5) DELETE (logical delete: row disappears from table, files removed logically)
# ----------------------------
print("\n[STEP 5] DELETE one row (logical delete; physical cleanup happens later)\n")

spark.sql(f"DELETE FROM {TABLE_NAME} WHERE order_id = 1")

spark.sql(f"SELECT * FROM {TABLE_NAME} ORDER BY order_id").show(truncate=False)

list_path("Table folder after DELETE", DBFS_PATH)
list_path("_delta_log after DELETE", f"{DBFS_PATH}/_delta_log")

# ----------------------------
# 6) MERGE (Upsert) - update existing + insert new in ONE transaction
# ----------------------------
print("\n[STEP 6] MERGE (upsert) in ONE transaction\n")

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

list_path("Table folder after MERGE", DBFS_PATH)
list_path("_delta_log after MERGE", f"{DBFS_PATH}/_delta_log")

# ----------------------------
# 7) HISTORY + Time Travel (prove versions exist)
# ----------------------------
print("\n[STEP 7] DESCRIBE HISTORY (shows versions/operations)\n")
spark.sql(f"DESCRIBE HISTORY {TABLE_NAME}").show(truncate=False)

# Pick a couple versions to time travel (adjust version numbers if needed)
print("\n[Time Travel] Read an older version (example: VERSION AS OF 1)\n")
try:
    spark.sql(f"SELECT * FROM {TABLE_NAME} VERSION AS OF 1 ORDER BY order_id").show(truncate=False)
except Exception as e:
    print("Time travel query failed (maybe version 1 doesn't exist in your run). Error:", e)

# ----------------------------
# 8) OPTIONAL: VACUUM (physical deletion)
# ----------------------------
# NOTE: For demo only. In production, do NOT use RETAIN 0 HOURS casually.
# It can break time travel and violates typical retention safety.
# Uncomment to demonstrate physical cleanup:
#
# print("\n[OPTIONAL] VACUUM to physically delete old files (demo only!)\n")
# spark.sql(f"VACUUM {TABLE_NAME} RETAIN 0 HOURS")
# list_path("Table folder after VACUUM", DBFS_PATH)
# list_path("_delta_log after VACUUM", f"{DBFS_PATH}/_delta_log")

print("\n✅ Demo complete.")
print("Key observation:")
print("- Each SQL statement above created a new commit JSON in _delta_log (one transaction).")
print("- UPDATE/DELETE/MERGE do NOT edit files in place: they add new files and logically remove old ones via the log.")
