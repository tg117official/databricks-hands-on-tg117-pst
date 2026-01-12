# ==========================================
# Databricks Filesystem & Storage Hands-on
# Single-script demo (safe + minimal)
# ==========================================

from pyspark.sql import functions as F
import time

print("Spark version:", spark.version)

# -----------------------------
# 0) Paths used in this demo
# -----------------------------
# DBFS path (persists within workspace storage layer)
dbfs_base = "dbfs:/tmp/tg117_fs_demo"
dbfs_csv_dir = f"{dbfs_base}/csv_out"
dbfs_parquet_dir = f"{dbfs_base}/parquet_out"
dbfs_text_file = f"{dbfs_base}/hello.txt"

# Local driver path (exists on the driver VM only; not shared/persistent)
local_base = "file:/tmp/tg117_local_demo"

print("\nDBFS base:", dbfs_base)
print("Local base:", local_base)

# -----------------------------
# 1) dbutils.fs basics
# -----------------------------
print("\n[1] Create DBFS directory")
dbutils.fs.mkdirs(dbfs_base)

print("\n[1] Write a small text file into DBFS")
dbutils.fs.put(dbfs_text_file, "Hello from Databricks DBFS!\n", overwrite=True)

print("\n[1] List DBFS directory")
for f in dbutils.fs.ls(dbfs_base):
    print(f.path, f.size)

print("\n[1] Read the text file back")
print(dbutils.fs.head(dbfs_text_file, 1000))

# -----------------------------
# 2) Local filesystem vs DBFS
# -----------------------------
print("\n[2] Create local directory (driver-only)")
dbutils.fs.mkdirs(local_base)  # yes, dbutils can create file:/ paths too

print("\n[2] Write a local file")
dbutils.fs.put(f"{local_base}/local_hello.txt", "Hello from the driver local disk!\n", overwrite=True)

print("\n[2] List local directory")
for f in dbutils.fs.ls(local_base):
    print(f.path, f.size)

print("\nNOTE:")
print("- file:/... is ONLY on the driver VM (can be lost if cluster terminates).")
print("- dbfs:/... is accessible as DBFS (more persistent).")

# -----------------------------
# 3) Create a DataFrame and display
# -----------------------------
print("\n[3] Create sample DataFrame")
data = [
    (1, "Sandeep", "Databricks"),
    (2, "Amit", "Azure"),
    (3, "Neha", "Spark"),
    (4, "Ravi", "Delta"),
]
df = spark.createDataFrame(data, ["id", "name", "topic"]).withColumn("ts", F.current_timestamp())
display(df)

# -----------------------------
# 4) Write DataFrame to DBFS (CSV + Parquet)
# -----------------------------
print("\n[4] Write as CSV (folder output)")
(df.coalesce(1)
   .write.mode("overwrite")
   .option("header", "true")
   .csv(dbfs_csv_dir))

print("[4] Write as Parquet")
(df.write.mode("overwrite").parquet(dbfs_parquet_dir))

print("\n[4] List DBFS output folders")
print("CSV output:")
for f in dbutils.fs.ls(dbfs_csv_dir):
    print("  ", f.path, f.size)

print("Parquet output:")
for f in dbutils.fs.ls(dbfs_parquet_dir):
    print("  ", f.path, f.size)

# -----------------------------
# 5) Read back from DBFS
# -----------------------------
print("\n[5] Read back Parquet")
df_parquet = spark.read.parquet(dbfs_parquet_dir)
display(df_parquet)

print("\n[5] Read back CSV")
df_csv = spark.read.option("header", "true").csv(dbfs_csv_dir)
display(df_csv)

# -----------------------------
# 6) Copy / Move / Remove examples
# -----------------------------
print("\n[6] Copy DBFS file to a new file")
dbfs_text_copy = f"{dbfs_base}/hello_copy.txt"
dbutils.fs.cp(dbfs_text_file, dbfs_text_copy)

print("[6] Verify copy exists")
for f in dbutils.fs.ls(dbfs_base):
    print("  ", f.path, f.size)

print("\n[6] Remove the copied file only")
dbutils.fs.rm(dbfs_text_copy)

print("[6] After removing copy")
for f in dbutils.fs.ls(dbfs_base):
    print("  ", f.path, f.size)

# -----------------------------
# 7) Exercise prompts (do these yourself)
# -----------------------------
print("\nEXERCISES (Try Now):")
print("1) Create a new folder under dbfs:/tmp/<yourname>/ and put 2 text files in it.")
print("2) Use dbutils.fs.ls to list sizes and identify the largest file.")
print("3) Write df as JSON to DBFS and read it back.")
print("4) Use dbutils.fs.rm(path, recurse=True) to clean up only YOUR folder.")
print("5) Create a second DataFrame, write both as Parquet, and union them after reading back.")

# -----------------------------
# 8) Optional cleanup (uncomment if you want)
# -----------------------------
# print("\n[Cleanup] Removing demo folder from DBFS...")
# dbutils.fs.rm(dbfs_base, recurse=True)
# print("Cleanup done.")
