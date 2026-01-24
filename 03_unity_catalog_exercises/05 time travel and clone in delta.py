# ============================================================
# DELTA TIME TRAVEL MASTER SCRIPT (ADLS + Azure Portal Visualization)
# Covers: HISTORY, VERSION/TIMESTAMP time travel, RESTORE rollback,
# CLONE (shallow/deep), snapshot backups, retention + VACUUM impact,
# and common scenarios.
# ============================================================

from pyspark.sql import SparkSession
import time
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# -----------------------------
# EXERCISE 0) Choose table + ADLS location (stable so you can inspect in Azure Portal)
# -----------------------------
CATALOG = "ecom_dev"
SCHEMA  = "delta_lab"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

TABLE = "demo_time_travel"

# Use your ADLS governed folder path
LOC = "abfss://data@pstdatademo.dfs.core.windows.net/uc_data/t1"

print("Using:", f"{CATALOG}.{SCHEMA}")
print("TABLE:", TABLE)
print("LOCATION:", LOC)

# STOP (Azure Portal):
# - Open the ADLS container in Azure Portal.
# - Navigate to path: uc_data/t1
# - Keep it open to observe:
#   _delta_log folder + parquet data files after every commit.


# -----------------------------
# EXERCISE 1) Clean start (metadata cleanup)
# -----------------------------
print("\n[EXERCISE 1] Clean start: DROP TABLE\n")
spark.sql(f"DROP TABLE IF EXISTS {TABLE}")

print("If you want a truly clean start, delete this folder manually in Azure Portal now:")
print("Path:", LOC)

# STOP:
# - In Azure Portal: delete the folder uc_data/t1 if you want a clean log from scratch.
# - Then continue.


# -----------------------------
# EXERCISE 2) Create Delta table at known LOCATION
# -----------------------------
print("\n[EXERCISE 2] Create Delta table at LOCATION\n")
spark.sql(f"""
CREATE TABLE {TABLE} (
  id INT,
  name STRING,
  score INT
)
USING DELTA
LOCATION '{LOC}'
""")

print("Azure Portal check:")
print("- _delta_log should appear under the LOCATION")

# STOP:
# - Explain: Delta anatomy = data files + _delta_log (JSON commits, checkpoints).


# -----------------------------
# Helpers (no filesystem ops)
# -----------------------------
def show_table(msg):
    print(f"\n--- {msg} ---")
    spark.sql(f"SELECT * FROM {TABLE} ORDER BY id").show(truncate=False)

def history_df():
    return spark.sql(f"DESCRIBE HISTORY {TABLE}")


# -----------------------------
# EXERCISE 3) Create multiple versions (each statement = 1 commit/version)
# -----------------------------
print("\n[EXERCISE 3] Create multiple versions (INSERT/UPDATE/DELETE/MERGE)\n")

# V1: Insert initial rows
spark.sql(f"INSERT INTO {TABLE} VALUES (1,'A',10),(2,'B',20),(3,'C',30)")
show_table("After initial INSERT (new version committed)")

# Capture timestamps for TIMESTAMP AS OF demo (best effort)
t_after_insert = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
time.sleep(2)

# V2: Update a row
spark.sql(f"UPDATE {TABLE} SET score = 99 WHERE id = 2")
show_table("After UPDATE id=2 (new version committed)")
t_after_update = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
time.sleep(2)

# V3: Delete a row
spark.sql(f"DELETE FROM {TABLE} WHERE id = 1")
show_table("After DELETE id=1 (new version committed)")
t_after_delete = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
time.sleep(2)

# V4: Merge (upsert)
spark.sql(f"""
MERGE INTO {TABLE} t
USING (SELECT 3 AS id, 'C' AS name, 300 AS score
       UNION ALL
       SELECT 4 AS id, 'D' AS name, 40 AS score) s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
show_table("After MERGE (update id=3, insert id=4)")
t_after_merge = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

print("\nAzure Portal check now:")
print("- _delta_log should have multiple new JSON commits (one per statement).")
print("- Data parquet files may increase due to copy-on-write rewrites.")

# STOP:
# - each statement is one atomic transaction -> one new version.


# -----------------------------
# EXERCISE 4) DESCRIBE HISTORY (audit trail)
# -----------------------------
print("\n[EXERCISE 4] DESCRIBE HISTORY (versions & operations)\n")
hist = history_df()
hist.show(50, truncate=False)

minv = hist.agg({"version": "min"}).collect()[0][0]
maxv = hist.agg({"version": "max"}).collect()[0][0]
print(f"\nVersion range: min={minv}, max={maxv}")

# STOP:
# - Explain what history columns mean: version, timestamp, operation, user, etc.


# -----------------------------
# EXERCISE 5) TIME TRAVEL READS (VERSION AS OF)
# -----------------------------
print("\n[EXERCISE 5] Time travel read by VERSION AS OF\n")

v_insert = int(minv) + 1 if int(minv) + 1 <= int(maxv) else int(minv)
v_latest = int(maxv)

print(f"\nRead table at VERSION AS OF {v_insert} (older snapshot):")
spark.sql(f"SELECT * FROM {TABLE} VERSION AS OF {v_insert} ORDER BY id").show(truncate=False)

print(f"\nRead table at VERSION AS OF {v_latest} (latest snapshot):")
spark.sql(f"SELECT * FROM {TABLE} VERSION AS OF {v_latest} ORDER BY id").show(truncate=False)

print("\nCompare older vs latest (approx diff):")
older_df = spark.sql(f"SELECT * FROM {TABLE} VERSION AS OF {v_insert}")
latest_df = spark.sql(f"SELECT * FROM {TABLE} VERSION AS OF {v_latest}")

print("\nRows in latest but not in older:")
latest_df.subtract(older_df).show(truncate=False)

print("\nRows in older but not in latest:")
older_df.subtract(latest_df).show(truncate=False)

# STOP:
# - VERSION AS OF is the most reliable “point-in-time” debugging tool.


# -----------------------------
# EXERCISE 6) TIME TRAVEL READS (TIMESTAMP AS OF)
# NOTE:
# - Timestamp precision/timezone can vary.
# - If it fails, use VERSION AS OF as the teaching fallback.
# -----------------------------
print("\n[EXERCISE 6] Time travel read by TIMESTAMP AS OF (best effort)\n")

print(f"\nRead snapshot around 'after INSERT' timestamp (UTC): {t_after_insert}")
try:
    spark.sql(f"SELECT * FROM {TABLE} TIMESTAMP AS OF '{t_after_insert}' ORDER BY id").show(truncate=False)
except Exception as e:
    print("TIMESTAMP AS OF read failed (timezone/precision can vary). Error:", str(e)[:300], "...")

print(f"\nRead snapshot around 'after UPDATE' timestamp (UTC): {t_after_update}")
try:
    spark.sql(f"SELECT * FROM {TABLE} TIMESTAMP AS OF '{t_after_update}' ORDER BY id").show(truncate=False)
except Exception as e:
    print("TIMESTAMP AS OF read failed. Error:", str(e)[:300], "...")

# STOP:
# - Timestamp travel is convenient for auditors, but version travel is safer in demos.


# -----------------------------
# EXERCISE 7) RESTORE (rollback)
# RESTORE creates a NEW version that matches an older version.
# It does NOT delete newer versions.
# -----------------------------
print("\n[EXERCISE 7] RESTORE TABLE to an older VERSION\n")

restore_to = v_insert
print(f"\nRestoring table to VERSION AS OF {restore_to}")
spark.sql(f"RESTORE TABLE {TABLE} TO VERSION AS OF {restore_to}")

show_table(f"After RESTORE to version {restore_to} (current state)")

print("\nHistory after RESTORE (RESTORE is a new operation/version):")
history_df().show(50, truncate=False)

# STOP:
# - RESTORE is like “rollback commit”, but history is still preserved.


# -----------------------------
# EXERCISE 8) CLONE scenarios (Shallow vs Deep)
# - SHALLOW CLONE: fast, shares underlying files (depends on them)
# - DEEP CLONE: full physical copy (more expensive)
# -----------------------------
print("\n[EXERCISE 8] CLONE scenarios (Shallow vs Deep)\n")

CLONE_SHALLOW = "demo_time_travel_clone_shallow"
CLONE_DEEP    = "demo_time_travel_clone_deep"

spark.sql(f"DROP TABLE IF EXISTS {CLONE_SHALLOW}")
spark.sql(f"DROP TABLE IF EXISTS {CLONE_DEEP}")

spark.sql(f"CREATE TABLE {CLONE_SHALLOW} SHALLOW CLONE {TABLE}")
print(f"\nShallow clone created: {CLONE_SHALLOW}")
spark.sql(f"SELECT * FROM {CLONE_SHALLOW} ORDER BY id").show(truncate=False)

# Deep clone is optional (can be slower/costly)
# Uncomment if you want to run:
# spark.sql(f"CREATE TABLE {CLONE_DEEP} DEEP CLONE {TABLE}")
# print(f"\nDeep clone created: {CLONE_DEEP}")
# spark.sql(f"SELECT * FROM {CLONE_DEEP} ORDER BY id").show(truncate=False)

# STOP:
# - Shallow clone is like “fast snapshot for testing”.
# - Deep clone is like “full backup copy”.


# -----------------------------
# EXERCISE 9) Frozen snapshot backup table (AS OF -> new table)
# Useful: compliance snapshots / point-in-time backups for reprocessing
# -----------------------------
print("\n[EXERCISE 9] Create backup table from a historical snapshot\n")

BACKUP_TABLE = "demo_time_travel_backup_v"
spark.sql(f"DROP TABLE IF EXISTS {BACKUP_TABLE}")

spark.sql(f"""
CREATE TABLE {BACKUP_TABLE}
USING DELTA
AS SELECT * FROM {TABLE} VERSION AS OF {restore_to}
""")

print(f"Backup table created from VERSION AS OF {restore_to}: {BACKUP_TABLE}")
spark.sql(f"SELECT * FROM {BACKUP_TABLE} ORDER BY id").show(truncate=False)

# STOP:
# - This is a practical “snapshot backup” pattern.


# -----------------------------
# EXERCISE 10) VACUUM impact on Time Travel (demo-only)
# - Aggressive vacuum can make old versions unrecoverable.
# - Databricks blocks low retention by default; we disable safety briefly.
# -----------------------------
print("\n[EXERCISE 10] VACUUM impact on time travel (demo-only)\n"
      "WARNING: RETAIN 0 HOURS is NOT recommended in production.\n")

spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql(f"VACUUM {TABLE} RETAIN 0 HOURS")
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")

print("Azure Portal check now:")
print("- Some old/removed parquet files may be physically deleted")
print("- Time travel/restore to older versions may fail now")

print(f"\nTry time travel AFTER aggressive VACUUM to VERSION AS OF {restore_to} (may fail):")
try:
    spark.sql(f"SELECT * FROM {TABLE} VERSION AS OF {restore_to} ORDER BY id").show(truncate=False)
    print("Time travel still worked (depends on which files were eligible and deleted).")
except Exception as e:
    print("Time travel failed after VACUUM (expected if required files were deleted).")
    print("Error:", str(e)[:400], "...")

print("\nTry RESTORE AFTER aggressive VACUUM (may fail):")
try:
    spark.sql(f"RESTORE TABLE {TABLE} TO VERSION AS OF {restore_to}")
    print("RESTORE succeeded (depends on what VACUUM removed).")
except Exception as e:
    print("RESTORE failed after VACUUM (expected if required files are gone).")
    print("Error:", str(e)[:400], "...")

# -----------------------------
# FINAL TEACHING TAKEAWAYS
# -----------------------------
print("\n============================================================")
print("TIME TRAVEL KEY TAKEAWAYS:")
print("1) Time travel reads are non-destructive: VERSION AS OF / TIMESTAMP AS OF.")
print("2) RESTORE makes an older snapshot current by creating a NEW version (history remains).")
print("3) DESCRIBE HISTORY is your audit trail (operations, timestamps, versions).")
print("4) CLONE helps testing/backup: SHALLOW is fast (shares files), DEEP copies data physically.")
print("5) VACUUM can permanently remove files needed for old versions -> old versions may become unrecoverable.")
print("============================================================\n")
