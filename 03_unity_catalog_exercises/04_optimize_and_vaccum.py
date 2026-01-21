# ============================================================
# DELTA OPTIMIZE + VACUUM DEMO (Single Script)
# Goal: Demonstrate the most common real-world situations:
# 1) Small files problem (many tiny writes) -> OPTIMIZE compaction
# 2) UPDATE/DELETE/MERGE create "removed" files logically -> still on storage
# 3) VACUUM physically deletes old/removed files (after retention window)
# 4) Time travel works before VACUUM, can break after aggressive VACUUM
#
# Recommended environment: Databricks notebook (because dbutils.fs.ls is easiest)
# ============================================================

from pyspark.sql import SparkSession
import shutil, os

spark = SparkSession.builder.getOrCreate()

TABLE_NAME = "demo_opt_vac"
DBFS_PATH  = "/dbfs/tmp/delta_opt_vac_demo/orders"     # physical driver path
DELTA_LOC  = "/dbfs/tmp/delta_opt_vac_demo/orders"     # used in SQL LOCATION
LOG_PATH   = f"{DBFS_PATH}/_delta_log"

# ----------------------------
# Helpers: list files + count parquet files (to "see" compaction/cleanup)
# ----------------------------
def ls(path):
    """List files using dbutils if available; fallback to os.walk."""
    print(f"\n--- LIST: {path} ---")
    try:
        # Databricks
        display(dbutils.fs.ls(path))
        return
    except Exception:
        pass

    # fallback for /dbfs paths
    if os.path.exists(path):
        for root, dirs, files in os.walk(path):
            rel = root.replace(path, ".")
            print(rel)
            for d in sorted(dirs):
                print("  [DIR] ", d)
            for f in sorted(files):
                print("  [FILE]", f)
    else:
        print("(path not found)")

def count_parquet_files(path):
    """Count parquet files under the table folder (excluding _delta_log)."""
    cnt = 0
    for root, dirs, files in os.walk(path):
        if "_delta_log" in root:
            continue
        for f in files:
            if f.endswith(".parquet"):
                cnt += 1
    return cnt

def show_history():
    print("\n--- DESCRIBE HISTORY (versions & operations) ---")
    spark.sql(f"DESCRIBE HISTORY {TABLE_NAME}").show(50, truncate=False)

# ----------------------------
# STEP 0) Clean start: drop table + delete folder
# ----------------------------
print("\n[STEP 0] Clean start\n")
spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")
try:
    shutil.rmtree(DBFS_PATH)
except Exception:
    pass

ls("/dbfs/tmp/delta_opt_vac_demo")

# ----------------------------
# STEP 1) Create a Delta table at a known LOCATION
# ----------------------------
print("\n[STEP 1] Create Delta table\n")
spark.sql(f"""
CREATE TABLE {TABLE_NAME} (
  order_id INT,
  amount   INT,
  status   STRING
)
USING DELTA
LOCATION '{DELTA_LOC}'
""")

ls(DBFS_PATH)
ls(LOG_PATH)

# ----------------------------
# STEP 2) Create the "small files problem" (many tiny appends)
# Common situation: streaming micro-batches or frequent small batch loads.
# Each small append typically creates at least one small parquet file.
# ----------------------------
print("\n[STEP 2] Create small files via many small appends\n")
for i in range(1, 21):  # 20 small batches
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES ({i}, {i*10}, 'CREATED')")

spark.sql(f"SELECT COUNT(*) AS rows FROM {TABLE_NAME}").show()
ls(DBFS_PATH)
ls(LOG_PATH)

before_opt_files = count_parquet_files(DBFS_PATH)
print(f"\nParquet file count BEFORE OPTIMIZE: {before_opt_files}")

# ----------------------------
# STEP 3) OPTIMIZE (Compaction)
# Common situation: fix small files, reduce tasks, improve scan performance.
# OPTIMIZE rewrites data into fewer larger files (copy-on-write).
# Old small files become logically removed (still on storage until VACUUM).
# ----------------------------
print("\n[STEP 3] OPTIMIZE to compact small files\n")
spark.sql(f"OPTIMIZE {TABLE_NAME}")

after_opt_files = count_parquet_files(DBFS_PATH)
print(f"\nParquet file count AFTER OPTIMIZE: {after_opt_files}")

# Observe: history shows OPTIMIZE, and _delta_log gets new commits/checkpoints eventually
show_history()
ls(DBFS_PATH)
ls(LOG_PATH)

# ----------------------------
# STEP 4) Create "removed files" using UPDATE + DELETE (typical CDC activity)
# Common situation: MERGE/UPDATE/DELETE rewrites files (copy-on-write)
# and marks old files removed in log, but files still exist physically.
# ----------------------------
print("\n[STEP 4] UPDATE + DELETE to create removed files (logical removals)\n")

spark.sql(f"UPDATE {TABLE_NAME} SET status='SHIPPED' WHERE order_id BETWEEN 5 AND 10")
spark.sql(f"DELETE FROM {TABLE_NAME} WHERE order_id IN (1,2,3)")

spark.sql(f"SELECT * FROM {TABLE_NAME} ORDER BY order_id").show(50, truncate=False)
show_history()

# At this point, table result is correct, but older files are still on storage.
ls(DBFS_PATH)
ls(LOG_PATH)

# ----------------------------
# STEP 5) Time travel demo (works before VACUUM)
# Common situation: auditing, debugging, rollback, reprocessing.
# ----------------------------
print("\n[STEP 5] Time travel BEFORE VACUUM (should work)\n")
hist = spark.sql(f"DESCRIBE HISTORY {TABLE_NAME}")
minv = hist.agg({"version":"min"}).collect()[0][0]
maxv = hist.agg({"version":"max"}).collect()[0][0]
print(f"History range: min version={minv}, max version={maxv}")

# Pick an older version (minv is often 0; choose something like minv+1 if available)
older = int(minv) + 1 if int(minv) + 1 <= int(maxv) else int(minv)
print(f"\nReading VERSION AS OF {older} (older snapshot):")
spark.sql(f"SELECT * FROM {TABLE_NAME} VERSION AS OF {older} ORDER BY order_id").show(50, truncate=False)

# ----------------------------
# STEP 6) VACUUM (Physical cleanup)
# Common situation: storage control + remove old files no longer needed.
# IMPORTANT:
# - Default retention is typically 7 days (safe).
# - VACUUM with very low retention can break time travel & streaming recovery.
# - Databricks usually blocks RETAIN < 168 hours unless you disable a safety check.
#
# We'll show TWO modes:
#   A) Safe/typical VACUUM (default) -> may not delete much in a fresh demo
#   B) Demo-only aggressive VACUUM (RETAIN 0 HOURS) -> will delete old files now
# ----------------------------
print("\n[STEP 6A] VACUUM (safe/default retention) — may delete little in a fresh demo\n")
spark.sql(f"VACUUM {TABLE_NAME}")  # default retention
ls(DBFS_PATH)

print("\n[STEP 6B] Demo-only aggressive VACUUM (RETAIN 0 HOURS)\n"
      "WARNING: This can break time travel and is NOT recommended in production.\n")

# Safety switch required on Databricks to allow <168 hours retention:
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

spark.sql(f"VACUUM {TABLE_NAME} RETAIN 0 HOURS")

# (Optional) turn safety back on after demo
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")

ls(DBFS_PATH)
ls(LOG_PATH)

# ----------------------------
# STEP 7) Time travel after aggressive VACUUM (may fail)
# This demonstrates the real consequence: old snapshots can become unreadable.
# ----------------------------
print("\n[STEP 7] Time travel AFTER aggressive VACUUM (may fail)\n")
try:
    spark.sql(f"SELECT * FROM {TABLE_NAME} VERSION AS OF {older} ORDER BY order_id").show(50, truncate=False)
    print("Time travel still worked (depends on what files were vacuumed).")
except Exception as e:
    print("Time travel failed after VACUUM (expected in aggressive cleanup).")
    print("Error:", str(e)[:500], "...")

# ----------------------------
# FINAL SUMMARY
# ----------------------------
print("\n============================================================")
print("KEY TAKEAWAYS (teach these):")
print("1) OPTIMIZE = performance: compacts many small parquet files into fewer large files.")
print("2) OPTIMIZE is copy-on-write: it adds new files + marks old ones removed in the log.")
print("3) UPDATE/DELETE/MERGE also rewrite files and create logically removed files.")
print("4) VACUUM = storage hygiene: physically deletes old/removed files AFTER retention window.")
print("5) Aggressive VACUUM can break time travel and streaming recovery (use carefully).")
print("============================================================\n")
