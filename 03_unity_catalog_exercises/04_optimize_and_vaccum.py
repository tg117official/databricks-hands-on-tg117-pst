# ============================================================
# DELTA OPTIMIZE + VACUUM DEMO (ADLS + Azure Portal Visualization)
# Goal: Demonstrate real-world situations:
# 1) Small files problem -> OPTIMIZE compaction
# 2) UPDATE/DELETE/MERGE create "removed" files logically -> still on storage
# 3) VACUUM physically deletes old/removed files (after retention window)
# 4) Time travel works before VACUUM, can break after aggressive VACUUM
#
# Run in: Databricks notebook, DEV schema
# ============================================================

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ----------------------------
# EXERCISE 0) UC context + choose ADLS location
# ----------------------------
CATALOG = "ecom_dev"
SCHEMA  = "delta_lab"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

TABLE_NAME = "demo_opt_vac"

# ADLS Delta folder (table will be created at this LOCATION)
# Replace with YOUR governed external path.
DELTA_LOC = "abfss://data@uclabdemo.dfs.core.windows.net/uc_lab/delta_opt_vac_demo/orders"

print("Using:", f"{CATALOG}.{SCHEMA}")
print("TABLE_NAME:", TABLE_NAME)
print("DELTA_LOC:", DELTA_LOC)

# STOP (Azure Portal setup):
# - Open the ADLS container in Azure Portal.
# - Navigate to the folder:
#   uc_lab/delta_opt_vac_demo/orders
# - Keep it open to observe:
#   _delta_log folder and parquet files after each exercise.


# ----------------------------
# EXERCISE 1) Clean start (metadata only)
# Goal:
# - Drop table
# Note:
# - Since this is an external LOCATION, DROP TABLE typically does NOT delete files.
# - For a clean folder, delete the folder manually in Azure Portal before continuing.
# ----------------------------
print("\n[EXERCISE 1] Clean start: DROP TABLE\n")
spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME}")

print("If you want a clean ADLS folder, delete it manually in Azure Portal now:")
print("Path:", DELTA_LOC)

# STOP:
# - Optionally delete the folder in Azure Portal to start fresh.


# ----------------------------
# EXERCISE 2) Create Delta table at known LOCATION
# Goal:
# - Create table pointing to ADLS folder
# - Observe _delta_log creation in Azure Portal
# ----------------------------
print("\n[EXERCISE 2] Create Delta table at LOCATION\n")
spark.sql(f"""
CREATE TABLE {TABLE_NAME} (
  order_id INT,
  amount   INT,
  status   STRING
)
USING DELTA
LOCATION '{DELTA_LOC}'
""")

print("Azure Portal check now:")
print("- _delta_log should appear under the LOCATION")

# STOP:
# - Explain: Delta = parquet files + _delta_log transaction log.


# ----------------------------
# EXERCISE 3) Create the "small files problem" (many tiny appends)
# Goal:
# - 20 small inserts (each insert is a separate transaction/version)
# - Observe many parquet files getting created (often) + log commits
# ----------------------------
print("\n[EXERCISE 3] Create small files via many small appends\n")

for i in range(1, 21):
    spark.sql(f"INSERT INTO {TABLE_NAME} VALUES ({i}, {i*10}, 'CREATED')")

spark.sql(f"SELECT COUNT(*) AS rows FROM {TABLE_NAME}").show()

print("Azure Portal check now:")
print("- You should see multiple parquet files (often many small ones)")
print("- _delta_log should have many commit JSON files (one per INSERT)")

# STOP:
# - streaming micro-batches / frequent small loads cause small files.


# ----------------------------
# EXERCISE 4) OPTIMIZE (Compaction)
# Goal:
# - Compact small files into fewer bigger files
# - OPTIMIZE is copy-on-write: it writes new files and marks old ones as removed in log
# ----------------------------
print("\n[EXERCISE 4] OPTIMIZE to compact small files\n")

spark.sql(f"OPTIMIZE {TABLE_NAME}")

print("OPTIMIZE executed (if supported). Now check:")
print("- Azure Portal: new larger parquet files may appear")
print("- Old small files may still physically exist (until VACUUM), but are logically removed")
print("- _delta_log has a new commit for OPTIMIZE")

print("\nDESCRIBE HISTORY after OPTIMIZE:")
spark.sql(f"DESCRIBE HISTORY {TABLE_NAME}").show(50, truncate=False)

# STOP:
# - OPTIMIZE improves performance (fewer files, fewer tasks).


# ----------------------------
# EXERCISE 5) UPDATE + DELETE (create logically removed files)
# Goal:
# - Typical CDC activity
# - Table results change, but old files stay on storage until VACUUM
# ----------------------------
print("\n[EXERCISE 5] UPDATE + DELETE (logical removals)\n")

spark.sql(f"UPDATE {TABLE_NAME} SET status='SHIPPED' WHERE order_id BETWEEN 5 AND 10")
spark.sql(f"DELETE FROM {TABLE_NAME} WHERE order_id IN (1,2,3)")

spark.sql(f"SELECT * FROM {TABLE_NAME} ORDER BY order_id").show(50, truncate=False)

print("\nDESCRIBE HISTORY after UPDATE + DELETE:")
spark.sql(f"DESCRIBE HISTORY {TABLE_NAME}").show(50, truncate=False)

print("Azure Portal check now:")
print("- More parquet files may appear (rewrite)")
print("- Old parquet files still exist physically")
print("- _delta_log has new commits for UPDATE and DELETE")

# STOP:
# - Key point: UPDATE/DELETE/MERGE never edit in place; they rewrite files.


# ----------------------------
# EXERCISE 6) Time travel BEFORE VACUUM (should work)
# Goal:
# - Capture an older version and query it
# ----------------------------
print("\n[EXERCISE 6] Time travel BEFORE VACUUM\n")

hist = spark.sql(f"DESCRIBE HISTORY {TABLE_NAME}")
minv = hist.agg({"version":"min"}).collect()[0][0]
maxv = hist.agg({"version":"max"}).collect()[0][0]
print(f"History range: min version={minv}, max version={maxv}")

older = int(minv) + 1 if int(minv) + 1 <= int(maxv) else int(minv)
print(f"\nReading VERSION AS OF {older} (older snapshot):")

try:
    spark.sql(f"SELECT * FROM {TABLE_NAME} VERSION AS OF {older} ORDER BY order_id").show(50, truncate=False)
except Exception as e:
    print("Time travel query failed (unexpected here unless permissions/retention issues).")
    print("Error:", str(e)[:500], "...")

# STOP:
# - time travel works because old files are still available.


# ----------------------------
# EXERCISE 7A) VACUUM (safe/default retention)
# Goal:
# - Show default VACUUM behavior
# - In a fresh demo, it may delete little because retention window isn't met
# ----------------------------
print("\n[EXERCISE 7A] VACUUM with default retention (safe)\n")
spark.sql(f"VACUUM {TABLE_NAME}")

print("Azure Portal check now:")
print("- You may see no change (common in fresh labs)")
print("- Because default retention is usually 7 days (168 hours)")

# STOP:
# - default VACUUM is safe but may not immediately delete files in a new table.


# ----------------------------
# EXERCISE 7B) Demo-only aggressive VACUUM (RETAIN 0 HOURS)
# Goal:
# - Physically delete old/removed files now
# WARNING:
# - NOT recommended in production
# - Disables retention safety check temporarily
# ----------------------------
print("\n[EXERCISE 7B] Demo-only aggressive VACUUM (RETAIN 0 HOURS)\n"
      "WARNING: This can break time travel and streaming recovery.\n")

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql(f"VACUUM {TABLE_NAME} RETAIN 0 HOURS")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")

print("Azure Portal check now:")
print("- Old/removed parquet files may be physically deleted")
print("- Some time travel versions may become unreadable")

# STOP:
# - In production, keep safe retention and plan vacuum carefully.


# ----------------------------
# EXERCISE 8) Time travel AFTER aggressive VACUUM (may fail)
# Goal:
# - Demonstrate consequence: older snapshot might fail
# ----------------------------
print("\n[EXERCISE 8] Time travel AFTER aggressive VACUUM (may fail)\n")

try:
    spark.sql(f"SELECT * FROM {TABLE_NAME} VERSION AS OF {older} ORDER BY order_id").show(50, truncate=False)
    print("Time travel still worked (depends on what files were vacuumed).")
except Exception as e:
    print("Time travel failed after VACUUM (expected in aggressive cleanup).")
    print("Error:", str(e)[:500], "...")

# ----------------------------
# FINAL SUMMARY (verbal teaching points)
# ----------------------------
print("\n============================================================")
print("KEY TAKEAWAYS:")
print("1) OPTIMIZE = compacts many small parquet files into fewer larger files.")
print("2) OPTIMIZE is copy-on-write: writes new files + marks old ones removed in _delta_log.")
print("3) UPDATE/DELETE/MERGE also rewrite files and create logically removed files.")
print("4) VACUUM = physically deletes old/removed files AFTER retention window.")
print("5) Aggressive VACUUM can break time travel and streaming recovery.")
print("============================================================\n")
