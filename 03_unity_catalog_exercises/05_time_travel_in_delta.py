# ============================================================
# DELTA TIME TRAVEL MASTER SCRIPT (Single Script)
# Covers: HISTORY, VERSION/TIMESTAMP time travel, AS OF reads,
# RESTORE rollback, CLONE (shallow/deep), snapshot backups,
# retention + VACUUM impact on time travel, and common scenarios.
#
# Best run in: Databricks notebook (dbutils.fs.ls works nicely).
# ============================================================

from pyspark.sql import SparkSession
import shutil, os, time
from datetime import datetime, timedelta

spark = SparkSession.builder.getOrCreate()

# -----------------------------
# 0) Choose table + location
# -----------------------------
TABLE = "demo_time_travel"
DBFS_PATH = "/dbfs/tmp/delta_time_travel_demo/t1"   # physical driver path for inspection
LOC = "/dbfs/tmp/delta_time_travel_demo/t1"         # used in SQL LOCATION
LOG = f"{DBFS_PATH}/_delta_log"

# -----------------------------
# Helpers (optional): list files so you can "see" log evolution
# -----------------------------
def ls(path):
    print(f"\n--- LIST: {path} ---")
    try:
        display(dbutils.fs.ls(path))
        return
    except Exception:
        pass
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

def show_table(msg):
    print(f"\n--- {msg} ---")
    spark.sql(f"SELECT * FROM {TABLE} ORDER BY id").show(truncate=False)

def history_df():
    return spark.sql(f"DESCRIBE HISTORY {TABLE}")

# -----------------------------
# 1) Clean start (drop + remove storage)
# -----------------------------
print("\n[STEP 1] Clean start\n")
spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
try:
    shutil.rmtree(DBFS_PATH)
except Exception:
    pass

# -----------------------------
# 2) Create Delta table at known LOCATION
# -----------------------------
print("\n[STEP 2] Create Delta table\n")
spark.sql(f"""
CREATE TABLE {TABLE} (
  id INT,
  name STRING,
  score INT
)
USING DELTA
LOCATION '{LOC}'
""")

ls(DBFS_PATH)
ls(LOG)

# -----------------------------
# 3) Make multiple versions (each statement = 1 commit/version)
# -----------------------------
print("\n[STEP 3] Create multiple versions (INSERT/UPDATE/DELETE/MERGE)\n")

# V1: Insert initial rows
spark.sql(f"INSERT INTO {TABLE} VALUES (1,'A',10),(2,'B',20),(3,'C',30)")
show_table("After initial INSERT (new version committed)")

# Capture a timestamp for TIMESTAMP AS OF demo (sleep to ensure different commit times)
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

# -----------------------------
# 4) DESCRIBE HISTORY (audit trail = your 'commit log')
# -----------------------------
print("\n[STEP 4] DESCRIBE HISTORY (versions & operations)\n")
hist = history_df()
hist.show(50, truncate=False)

# Get min/max versions for later use
minv = hist.agg({"version": "min"}).collect()[0][0]
maxv = hist.agg({"version": "max"}).collect()[0][0]
print(f"\nVersion range: min={minv}, max={maxv}")

# -----------------------------
# 5) TIME TRAVEL READS (Version AS OF)
# -----------------------------
print("\n[STEP 5] Time travel read by VERSION AS OF\n")

# Choose a couple versions safely:
v_insert = int(minv) + 1 if int(minv) + 1 <= int(maxv) else int(minv)
v_latest = int(maxv)

print(f"\nRead table at VERSION AS OF {v_insert} (older snapshot):")
spark.sql(f"SELECT * FROM {TABLE} VERSION AS OF {v_insert} ORDER BY id").show(truncate=False)

print(f"\nRead table at VERSION AS OF {v_latest} (latest snapshot):")
spark.sql(f"SELECT * FROM {TABLE} VERSION AS OF {v_latest} ORDER BY id").show(truncate=False)

# Typical scenario: compare two snapshots (debugging)
print("\nCompare older vs latest (example: what changed between versions?)")
older_df = spark.sql(f"SELECT * FROM {TABLE} VERSION AS OF {v_insert}")
latest_df = spark.sql(f"SELECT * FROM {TABLE} VERSION AS OF {v_latest}")

print("\nRows in latest but not in older (approx diff):")
latest_df.subtract(older_df).show(truncate=False)

print("\nRows in older but not in latest (approx diff):")
older_df.subtract(latest_df).show(truncate=False)

# -----------------------------
# 6) TIME TRAVEL READS (Timestamp AS OF)
# NOTE: Timestamp must be within history retention and before VACUUM removes files.
# -----------------------------
print("\n[STEP 6] Time travel read by TIMESTAMP AS OF\n")

# We captured UTC timestamps right after certain operations (best effort demo)
# Depending on workspace time settings, you may need to adjust to your timezone.
# These reads are "as-of" points-in-time.
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

# -----------------------------
# 7) RESTORE (Make an older snapshot the current table)
# RESTORE creates a NEW version whose state matches the chosen older version.
# It DOES NOT delete newer versions.
# -----------------------------
print("\n[STEP 7] RESTORE TABLE to an older VERSION\n")

# Pick a version to restore to (often the version after first insert is a clean demo)
restore_to = v_insert
print(f"\nRestoring table to VERSION AS OF {restore_to}")
spark.sql(f"RESTORE TABLE {TABLE} TO VERSION AS OF {restore_to}")

show_table(f"After RESTORE to version {restore_to} (current state)")
print("\nHistory after RESTORE (notice RESTORE is a new operation/version):")
history_df().show(50, truncate=False)

# -----------------------------
# 8) CLONE (Snapshot copy for testing/backup)
# SHALLOW CLONE: fast, shares underlying files (depends on them)
# DEEP CLONE: full physical copy (more expensive)
# -----------------------------
print("\n[STEP 8] CLONE scenarios (Shallow vs Deep)\n")

CLONE_SHALLOW = "demo_time_travel_clone_shallow"
CLONE_DEEP = "demo_time_travel_clone_deep"

spark.sql(f"DROP TABLE IF EXISTS {CLONE_SHALLOW}")
spark.sql(f"DROP TABLE IF EXISTS {CLONE_DEEP}")

# Shallow clone (metadata clone): good for quick testing
spark.sql(f"CREATE TABLE {CLONE_SHALLOW} SHALLOW CLONE {TABLE}")
print(f"\nShallow clone created: {CLONE_SHALLOW}")
spark.sql(f"SELECT * FROM {CLONE_SHALLOW} ORDER BY id").show(truncate=False)

# Deep clone (full copy): good for long-term backup or isolation (can be expensive)
# Uncomment if you want to run; it may take longer depending on table size.
# spark.sql(f"CREATE TABLE {CLONE_DEEP} DEEP CLONE {TABLE}")
# print(f"\nDeep clone created: {CLONE_DEEP}")
# spark.sql(f"SELECT * FROM {CLONE_DEEP} ORDER BY id").show(truncate=False)

# -----------------------------
# 9) "Frozen snapshot" backup table (AS OF -> new independent table)
# Useful for compliance snapshots / point-in-time backups
# -----------------------------
print("\n[STEP 9] Create backup table from a historical snapshot\n")
BACKUP_TABLE = "demo_time_travel_backup_v"
spark.sql(f"DROP TABLE IF EXISTS {BACKUP_TABLE}")

# Create a backup from the chosen historical version
spark.sql(f"""
CREATE TABLE {BACKUP_TABLE}
USING DELTA
AS SELECT * FROM {TABLE} VERSION AS OF {restore_to}
""")

print(f"Backup table created from VERSION AS OF {restore_to}: {BACKUP_TABLE}")
spark.sql(f"SELECT * FROM {BACKUP_TABLE} ORDER BY id").show(truncate=False)

# -----------------------------
# 10) VACUUM IMPACT on Time Travel (Demo-only)
# IMPORTANT:
# - Time travel/restore needs old files.
# - VACUUM deletes old (unreferenced) files AFTER retention.
# - Aggressive VACUUM can make old versions unrecoverable.
# ----------------------------------------------------------
print("\n[STEP 10] VACUUM impact on time travel (demo-only)\n"
      "We will attempt an aggressive VACUUM to show that old versions can become unreadable.\n"
      "DO NOT use RETAIN 0 HOURS in production casually.\n")

# Disable safety check to allow <168 hours retention (Databricks guardrail)
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# Aggressive vacuum (may break time travel for older versions)
spark.sql(f"VACUUM {TABLE} RETAIN 0 HOURS")

# Re-enable safety
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")

ls(DBFS_PATH)
ls(LOG)

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
    print("RESTORE succeeded (same caveat: depends on what vacuum removed).")
except Exception as e:
    print("RESTORE failed after VACUUM (expected if required files are gone).")
    print("Error:", str(e)[:400], "...")

# -----------------------------
# 11) Final Notes
# -----------------------------
print("\n============================================================")
print("TIME TRAVEL KEY TAKEAWAYS:")
print("1) Time travel reads are non-destructive: VERSION AS OF / TIMESTAMP AS OF.")
print("2) RESTORE makes an older snapshot current by creating a NEW version (does not delete history).")
print("3) DESCRIBE HISTORY is your audit trail (operations, user, timestamps).")
print("4) CLONE helps testing/backup: SHALLOW is fast (shares files), DEEP copies data physically.")
print("5) VACUUM can permanently remove files needed for old versions; after VACUUM those versions may be unrecoverable.")
print("============================================================\n")
