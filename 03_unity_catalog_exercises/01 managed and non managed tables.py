# ============================================================
# Databricks / Unity Catalog Hands-on (1 hour)
# Topic: Managed Tables vs External Tables (Delta)
#
# IMPORTANT:
# - This notebook/script is designed to be run STEP-BY-STEP (exercise-wise),
#   not in one go.
# - Operations are unchanged (same create/write/describe/drop/recreate flow).
# - Wherever filesystem checks are needed, you will VERIFY IN AZURE PORTAL
#   (graphical) instead of relying only on notebook output.
#
# Safety:
# - Run ONLY in a DEV schema.
# - We DROP tables to observe behavior.
# ============================================================

from pyspark.sql import functions as F
import time

# ------------------------------------------------------------
# Exercise 0: Set variables once (change as per your environment)
# ------------------------------------------------------------
CATALOG = "ecom_dev"                 # <-- your catalog
SCHEMA  = "uc_lab"                   # <-- your dev/lab schema
EXTERNAL_LOCATION_NAME = "ext_ecom_bronze"  # <-- UC External Location name

# We'll use unique names to avoid collisions across runs
suffix = str(int(time.time()))
MANAGED_TABLE  = f"orders_managed_{suffix}"
EXTERNAL_TABLE = f"orders_external_{suffix}"

managed_fqn  = f"{CATALOG}.{SCHEMA}.{MANAGED_TABLE}"
external_fqn = f"{CATALOG}.{SCHEMA}.{EXTERNAL_TABLE}"

print("Planned objects:")
print(" Managed table :", managed_fqn)
print(" External table:", external_fqn)

# ============================================================
# Exercise 1: Create schema (lab) + set context
# Goal:
# - Ensure schema exists
# - Switch to correct catalog/schema
# ============================================================
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print("Using:", f"{CATALOG}.{SCHEMA}")

# STOP HERE:
# - Show students catalog/schema in UI (Data Explorer) to confirm context.


# ============================================================
# Exercise 2: Create a sample dataset (same data for both tables)
# Goal:
# - Build df with timestamp, ingest_ts, amount
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

df = (
    spark.createDataFrame(sample, cols)
      .withColumn("order_date", F.to_timestamp("order_date"))
      .withColumn("ingest_ts", F.current_timestamp())
      .withColumn("amount", F.col("quantity") * F.col("price"))
)

df.cache()
df.show(truncate=False)
df.printSchema()

# STOP HERE:
# - Ask: “What extra columns did we add and why?”


# ============================================================
# Exercise 3: Create a MANAGED Delta table
# Goal:
# - Create managed table WITHOUT explicit LOCATION
# - Confirm read works
# ============================================================
df.write.format("delta").mode("overwrite").saveAsTable(managed_fqn)
print("Managed table created:", managed_fqn)

spark.sql(f"SELECT * FROM {managed_fqn} ORDER BY order_id").show(truncate=False)

# STOP HERE:
# - Tell students: We did NOT provide LOCATION => Databricks/UC decides storage under metastore root.
# - Next we will INSPECT metadata to find the Location.


# ============================================================
# Exercise 4: Inspect MANAGED table metadata (Type / Location)
# Goal:
# - Use DESCRIBE EXTENDED to see the managed table location
# - (Optionally) capture location for later discussion/demo
# ============================================================
managed_desc = spark.sql(f"DESCRIBE EXTENDED {managed_fqn}")
managed_desc.show(200, truncate=False)

# Optional: capture managed location for later explanation (no operation change)
def extract_kv(describe_df, keys):
    rows = describe_df.collect()
    out = {}
    for r in rows:
        k = str(r[0]).strip()
        if k in keys:
            out[k] = r[1]
    return out

keys = {"Provider", "Location", "Type", "Comment"}
m_kv = extract_kv(managed_desc, keys)
managed_location = m_kv.get("Location")

print("Managed key info:", m_kv)
print("Managed Location captured for teaching:", managed_location)

# STOP HERE:
# - In UI / Data Explorer: open the table -> look at details.
# - Explain: Managed table location is controlled by UC/metastore storage root.
# - (If you want) show the location string and tell students we’ll compare it with external.


# ============================================================
# Exercise 5: Inspect External Locations in UC (metadata only)
# Goal:
# - List external locations
# - Describe your external location to find its base URL
# ============================================================
spark.sql("SHOW EXTERNAL LOCATIONS").show(truncate=False)
spark.sql(f"DESCRIBE EXTERNAL LOCATION {EXTERNAL_LOCATION_NAME}").show(truncate=False)

# STOP HERE (Azure Portal demo setup):
# - In Azure Portal, open the Storage Account / Container related to external location.
# - Explain: External Location is a governed “approved base path” + credential in UC.


# ============================================================
# Exercise 6: Create an EXTERNAL Delta table (write to ADLS path + CREATE TABLE LOCATION)
# Goal:
# - Extract external location URL (or use the manual override you already have)
# - Write delta files to the ADLS path
# - Create table pointing to that path via LOCATION
# ============================================================

# Extract external location URL
ext_loc_info = spark.sql(f"DESCRIBE EXTERNAL LOCATION {EXTERNAL_LOCATION_NAME}").collect()

ext_url = None
for r in ext_loc_info:
    if str(r[0]).strip().lower() == "url":
        ext_url = r[1]
        break

if not ext_url:
    raise Exception(
        "Could not extract URL for external location. "
        "Open output of DESCRIBE EXTERNAL LOCATION and manually set ext_url."
    )

# Build a folder under that URL (this line stays, but note you override below)
external_data_path = f"{ext_url.rstrip('/')}/uc_lab/external_tables/{EXTERNAL_TABLE}"

# Your manual override (kept as-is, since you already used it successfully)
external_data_path = f"abfss://data@uclabdemo.dfs.core.windows.net/uc_lab/external_tables/{EXTERNAL_TABLE}"

print("External table data path:", external_data_path)

# Write delta files to that external path
df.write.format("delta").mode("overwrite").save(external_data_path)

# Create external table pointing to same path
spark.sql(f"""
CREATE TABLE {external_fqn}
USING DELTA
LOCATION '{external_data_path}'
""")

print("External table created:", external_fqn)
spark.sql(f"SELECT * FROM {external_fqn} ORDER BY order_id").show(truncate=False)

# STOP HERE (Azure Portal demo):
# - Go to Azure Portal -> storage account -> container -> path:
#   uc_lab/external_tables/<EXTERNAL_TABLE>
# - Show students the delta folder/files got created at the exact path they supplied.


# ============================================================
# Exercise 7: Inspect EXTERNAL table metadata (Type / Location)
# Goal:
# - DESCRIBE EXTENDED and verify Location matches ADLS path
# ============================================================
external_desc = spark.sql(f"DESCRIBE EXTENDED {external_fqn}")
external_desc.show(200, truncate=False)

e_kv = extract_kv(external_desc, keys)
print("External key info:", e_kv)

# STOP HERE:
# - Compare "Location" of managed vs external (students should see different roots)
# - Key teaching moment: managed location is UC-controlled; external is user-controlled path.


# ============================================================
# Exercise 8: The MOST important test - DROP TABLE behavior
# Goal:
# - Drop both tables (metadata removal)
# - Then prove external data still exists by reading from the LOCATION
# ============================================================

print("Managed count:", spark.sql(f"SELECT COUNT(*) AS c FROM {managed_fqn}").collect()[0]["c"])
print("External count:", spark.sql(f"SELECT COUNT(*) AS c FROM {external_fqn}").collect()[0]["c"])

spark.sql(f"DROP TABLE {managed_fqn}")
spark.sql(f"DROP TABLE {external_fqn}")
print("Dropped both tables.")

# STOP HERE (Azure Portal demo):
# - External path: check in Azure Portal -> files should STILL be present.
# - Managed path: you typically won’t browse it in Azure (it’s under metastore root / managed storage).
#   Explain conceptually: managed lifecycle is controlled by UC; drop usually deletes files.


# ============================================================
# Exercise 9: Prove external data still exists (read Delta from path + recreate table)
# Goal:
# - Read delta directly from external_data_path (no table)
# - Re-create the table pointing to same LOCATION
# ============================================================
df_external_direct = spark.read.format("delta").load(external_data_path)
df_external_direct.show(truncate=False)

spark.sql(f"""
CREATE TABLE {external_fqn}
USING DELTA
LOCATION '{external_data_path}'
""")

print("External table recreated:", external_fqn)
spark.sql(f"SELECT COUNT(*) AS c FROM {external_fqn}").show()

# STOP HERE:
# - Teaching: External table metadata can be dropped/recreated without losing the lake data.


# ============================================================
# Exercise 10 (Optional teaching): Managed location load check idea
# Goal:
# - Explain how you WOULD test managed deletion behavior, without changing current ops.
#
# Note:
# - We already captured managed_location earlier.
# - In many managed cases, reading delta from that location after DROP will fail
#   because underlying files got deleted.
#
# If you want to demonstrate this in a separate run, you can:
#   1) Create managed table
#   2) Capture managed_location
#   3) DROP TABLE
#   4) Try spark.read.format("delta").load(managed_location)
# ============================================================
print("Optional managed location captured earlier (for teaching):", managed_location)


# ============================================================
# Summary notes (for verbal revision)
# ============================================================
# Managed Table:
# - UC owns metadata + decides storage location
# - DROP TABLE usually removes metadata + deletes underlying data files
#
# External Table:
# - UC owns metadata/permissions, data stored at user-controlled path (ADLS)
# - DROP TABLE removes only metadata; files remain
# - Can recreate table from same LOCATION
#
# Medallion recommendation (ADLS lakehouse):
# - Bronze/Silver often external
# - Gold depends: managed or external based on lifecycle/governance
# ============================================================
print("Lab steps prepared for exercise-wise execution.")
