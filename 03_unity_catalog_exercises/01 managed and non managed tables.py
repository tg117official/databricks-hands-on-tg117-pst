
# ============================================================
# Databricks / Unity Catalog Hands-on (1 hour)
# Topic: Managed Tables vs External Tables (Delta)
#
# What you will learn:
# 1) What "managed" and "external" mean in Unity Catalog (UC)
# 2) Where data files live for each type
# 3) What happens to DATA when you DROP a table
# 4) How to inspect table metadata + storage location
#
# Prerequisites (already assumed from your setup):
# - Unity Catalog is enabled and you can see catalogs/schemas.
# - You have a catalog + schema where you can CREATE tables.
# - You have an External Location configured for ADLS (for external tables).
#
# IMPORTANT SAFETY:
# - Run this in a DEV schema only (not production).
# - We will DROP tables to observe behavior.
# ============================================================

from pyspark.sql import functions as F

# ------------------------------------------------------------
# 0) Set these 3 variables once (change as per your environment)
# ------------------------------------------------------------
CATALOG = "ecom_dev"          # <-- your catalog
SCHEMA  = "uc_lab"            # <-- a dev/lab schema (we can create it)
EXTERNAL_LOCATION_NAME = "ext_ecom_bronze"  # <-- your UC external location name

# We'll use unique names to avoid collisions across runs
import time
suffix = str(int(time.time()))
MANAGED_TABLE  = f"orders_managed_{suffix}"
EXTERNAL_TABLE = f"orders_external_{suffix}"


# ------------------------------------------------------------
# 1) Create schema (lab) and switch context
# ------------------------------------------------------------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print("Using:", f"{CATALOG}.{SCHEMA}")



# ============================================================
# 2) Create sample dataset (we will write the SAME data to both tables)
# ============================================================

sample = [
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

df = spark.createDataFrame(sample, cols) \
    .withColumn("order_date", F.to_timestamp("order_date")) \
    .withColumn("ingest_ts", F.current_timestamp()) \
    .withColumn("amount", F.col("quantity") * F.col("price"))

df.cache()
df.show(truncate=False)
df.printSchema()



# ============================================================
# 3) Create a MANAGED table
# ============================================================
# What is a managed table?
# - UC manages BOTH metadata and data files location.
# - Storage location is chosen/controlled by Databricks (metastore storage root).
# - Typical usage: curated/internal datasets where you want UC to own lifecycle.
# - DROP TABLE generally removes metadata AND (in most managed cases) the data files.

managed_fqn = f"{CATALOG}.{SCHEMA}.{MANAGED_TABLE}"

# We'll write as Delta managed table using saveAsTable (no explicit LOCATION)
df.write.format("delta").mode("overwrite").saveAsTable(managed_fqn)

print("Managed table created:", managed_fqn)

# Quick check
spark.sql(f"SELECT * FROM {managed_fqn} ORDER BY order_id").show(truncate=False)



# ============================================================
# 4) Inspect MANAGED table metadata & location
# ============================================================
# You should always learn to verify whether a table is managed or external
# using DESCRIBE EXTENDED / SHOW CREATE TABLE / information_schema.

managed_desc = spark.sql(f"DESCRIBE EXTENDED {managed_fqn}")
managed_desc.show(200, truncate=False)

# This often includes:
# - Type / Provider
# - Location
# - Table Properties
# Look for "Location" line and table type hints.



# ============================================================
# 5) Create an EXTERNAL table
# ============================================================
# What is an external table?
# - UC manages metadata + permissions BUT data files are stored at a user-controlled location (ADLS).
# - That location should be governed via UC External Location (recommended).
# - DROP TABLE usually removes metadata, but DOES NOT delete the underlying data files.
#
# We'll:
# A) Identify an allowed external path from the UC External Location
# B) Write data to that path as Delta
# C) Create a table pointing to that path (LOCATION)

# Step A: get the "URL" or "path" for your External Location
# In UC SQL, you can inspect external locations:
spark.sql("SHOW EXTERNAL LOCATIONS").show(truncate=False)

# Find the specific external location details:
spark.sql(f"DESCRIBE EXTERNAL LOCATION {EXTERNAL_LOCATION_NAME}").show(truncate=False)

# You will see something like:
# url = abfss://container@account.dfs.core.windows.net/some/base/path
#
# We'll build a folder under that URL for our external table data:
ext_loc_info = spark.sql(f"DESCRIBE EXTERNAL LOCATION {EXTERNAL_LOCATION_NAME}").collect()

# Extract the URL from describe output (works when output includes key/value rows).
# We search for the row where col_name == 'url'
ext_url = None
for r in ext_loc_info:
    # r has fields: col_name, data_type, comment for DESCRIBE-like outputs
    if str(r[0]).strip().lower() == "url":
        ext_url = r[1]
        break

if not ext_url:
    raise Exception(
        "Could not extract URL for external location. "
        "Open output of DESCRIBE EXTERNAL LOCATION and manually set ext_url."
    )

external_data_path = f"{ext_url.rstrip('/')}/uc_lab/external_tables/{EXTERNAL_TABLE}"


external_data_path = f"abfss://data@uclabdemo.dfs.core.windows.net/uc_lab/external_tables/{EXTERNAL_TABLE}"


print("External table data path:", external_data_path)

# Step B: write Delta files to that path
df.write.format("delta").mode("overwrite").save(external_data_path)

# Step C: Create external table with LOCATION
external_fqn = f"{CATALOG}.{SCHEMA}.{EXTERNAL_TABLE}"

spark.sql(f"""
CREATE TABLE {external_fqn}
USING DELTA
LOCATION '{external_data_path}'
""")

print("External table created:", external_fqn)

spark.sql(f"SELECT * FROM {external_fqn} ORDER BY order_id").show(truncate=False)


# ============================================================
# 6) Inspect EXTERNAL table metadata & location
# ============================================================

external_desc = spark.sql(f"DESCRIBE EXTENDED {external_fqn}")
external_desc.show(200, truncate=False)

# You should see the "Location" as the ADLS path you provided.
# This is the most visible difference in metadata.


# ============================================================
# 7) Compare side-by-side (Key differences you should observe)
# ============================================================
# We'll extract only key fields from DESCRIBE EXTENDED:
# - Location
# - Provider
# - Table properties
#
# In real projects, you might standardize these checks.

def extract_kv(describe_df, keys):
    rows = describe_df.collect()
    out = {}
    for r in rows:
        k = str(r[0]).strip()
        if k in keys:
            out[k] = r[1]
    return out

keys = {"Provider", "Location", "Type", "Comment"}  # depending on output format
m_kv = extract_kv(managed_desc, keys)
e_kv = extract_kv(external_desc, keys)

print("Managed key info:", m_kv)
print("External key info:", e_kv)


# ============================================================
# 8) The MOST important test: DROP TABLE behavior
# ============================================================
# We'll drop each table and observe what remains:
# - For managed: metadata removed; data is typically removed as well.
# - For external: metadata removed; data SHOULD remain at the LOCATION.
#
# NOTE:
# - There can be platform settings/policies that affect physical deletion.
# - But the conceptual rule for external tables remains: UC does not own the file lifecycle.

# Before dropping, let's confirm data exists in both
print("Managed count:", spark.sql(f"SELECT COUNT(*) AS c FROM {managed_fqn}").collect()[0]["c"])
print("External count:", spark.sql(f"SELECT COUNT(*) AS c FROM {external_fqn}").collect()[0]["c"])

# Drop metadata
spark.sql(f"DROP TABLE {managed_fqn}")
spark.sql(f"DROP TABLE {external_fqn}")

print("Dropped both tables.")


# ============================================================
# 9) Re-create the EXTERNAL table again from the same LOCATION (proof data still exists)
# ============================================================
# If data files are still in ADLS path, we can recreate the table.
# This is one of the biggest advantages of external tables:
# - You can drop/recreate metadata without losing data.
#
# We'll try to read Delta directly from the location first.

# Read delta files directly (no table)
df_external_direct = spark.read.format("delta").load(external_data_path)
df_external_direct.show(truncate=False)

# Now re-create the external table pointing to same location
spark.sql(f"""
CREATE TABLE {external_fqn}
USING DELTA
LOCATION '{external_data_path}'
""")

print("External table recreated:", external_fqn)
spark.sql(f"SELECT COUNT(*) AS c FROM {external_fqn}").show()

# ============================================================
# 10) Managed table data check (optional)
# ============================================================
# For managed table, after DROP TABLE, trying to read from its previous location is not easy
# unless you captured the "Location" from DESCRIBE EXTENDED before dropping.
#
# If you want to test it, capture managed location BEFORE drop and attempt to load delta.
# We'll show how to do it quickly in a future run:
#
# managed_location = m_kv.get("Location")
# spark.read.format("delta").load(managed_location).show()
#
# In many managed cases, load will fail because files were deleted (that's the point).
# ============================================================


# ============================================================
# 11) Summary notes (keep these as revision points)
# ============================================================
# Managed Table:
# - UC owns metadata + chooses/controls storage location
# - Simple for internal datasets
# - DROP TABLE often deletes underlying data files too
# - Best for curated tables where lifecycle should be controlled centrally
#
# External Table:
# - UC owns metadata/permissions, but DATA is at a path you control (ADLS)
# - Works best with "External Locations" (governed access)
# - DROP TABLE removes metadata but keeps data files
# - Best for lake/lakehouse patterns where data must outlive table metadata
#
# Practical recommendation for a Medallion architecture on ADLS:
# - Bronze/Silver often external (data is in the lake, managed by your storage strategy)
# - Gold can be managed or external depending on governance + lifecycle needs
#
# Cleanup (optional):
# - If you want to remove the external data files, delete that ADLS folder manually
#   (or use a controlled process). UC won't automatically delete it.
# ============================================================
print("Lab complete")


