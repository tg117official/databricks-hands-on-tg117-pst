# ============================================================
# DELTA Z-ORDER DEMO (Single Script)
# No filesystem listing. Focus: observe performance via "files read".
#
# What you will learn:
# 1) Why Z-ORDER exists (better data skipping on non-partition columns)
# 2) Difference: OPTIMIZE (compaction) vs OPTIMIZE ZORDER (clustering)
# 3) When Z-ORDER helps (filters/ranges on chosen columns)
# 4) When Z-ORDER may NOT help (random/high-cardinality keys)
#
# Run in Databricks Notebook (SQL + PySpark).
# ============================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

spark = SparkSession.builder.getOrCreate()

# -----------------------------
# 0) Clean start
# -----------------------------
TABLE = "student_zorder_orders"
LOC   = "/tmp/delta_student_zorder/orders"   # simple demo path (change if needed)

spark.sql(f"DROP TABLE IF EXISTS {TABLE}")

# -----------------------------
# 1) Create a Delta table (partitioned by order_date)
# Partitioning helps date filters. ZORDER will help filters on customer_id.
# -----------------------------
spark.sql(f"""
CREATE TABLE {TABLE} (
  order_id     STRING,
  customer_id  INT,
  order_date   DATE,
  amount       DOUBLE,
  city         STRING
)
USING DELTA
PARTITIONED BY (order_date)
LOCATION '{LOC}'
""")

# -----------------------------
# 2) Insert data in many small batches (creates many small files)
# This simulates streaming micro-batches or frequent small loads.
# -----------------------------
# 10 days of data, 10 micro-batches per day, 1000 rows each micro-batch
for day in range(1, 11):
    d = f"2026-01-{day:02d}"
    for b in range(10):
        spark.sql(f"""
        INSERT INTO {TABLE}
        SELECT
          concat('o_', '{d}', '_', cast({b} as string), '_', cast(id as string)) AS order_id,
          cast((id % 5000) as int) AS customer_id,
          date('{d}') AS order_date,
          cast((id % 1000) * 1.0 as double) AS amount,
          case when (id % 5)=0 then 'Pune'
               when (id % 5)=1 then 'Mumbai'
               when (id % 5)=2 then 'Nagpur'
               when (id % 5)=3 then 'Nashik'
               else 'Thane' end AS city
        FROM range(0, 1000)
        """)
print("Data loaded.")

# -----------------------------
# 3) Helper: count how many data files were read by a query
# Data skipping benefit of ZORDER is visible as fewer files read.
# -----------------------------
def files_read(sql_text, label):
    df = spark.sql(sql_text)
    df.count()  # force execution
    n = len(df.inputFiles())
    print(f"{label} -> files read = {n}")
    return n

# -----------------------------
# 4) Baseline queries BEFORE OPTIMIZE / ZORDER
# A) Date filter uses partition pruning (already good)
# B) customer_id filter (non-partition) depends on data skipping
# -----------------------------
q_date = f"""
SELECT * FROM {TABLE}
WHERE order_date = date('2026-01-05')
"""
files_read(q_date, "A) Date filter only (partition pruning)")

q_customer = f"""
SELECT * FROM {TABLE}
WHERE order_date BETWEEN date('2026-01-03') AND date('2026-01-07')
  AND customer_id = 42
"""
before = files_read(q_customer, "B) customer_id filter BEFORE OPTIMIZE/ZORDER")

# -----------------------------
# 5) OPTIMIZE without ZORDER (compaction only)
# This reduces small files, but does not strongly cluster customer_id values.
# -----------------------------
spark.sql(f"""
OPTIMIZE {TABLE}
WHERE order_date BETWEEN date('2026-01-03') AND date('2026-01-07')
""")

after_opt = files_read(q_customer, "C) customer_id filter AFTER OPTIMIZE (no ZORDER)")

# -----------------------------
# 6) OPTIMIZE with ZORDER(customer_id)
# This clusters customer_id values in fewer files -> better data skipping.
# -----------------------------
spark.sql(f"""
OPTIMIZE {TABLE}
WHERE order_date BETWEEN date('2026-01-03') AND date('2026-01-07')
ZORDER BY (customer_id)
""")

after_z = files_read(q_customer, "D) customer_id filter AFTER ZORDER(customer_id)")

print("\nSummary for customer_id filter:")
print("Before OPTIMIZE/ZORDER:", before)
print("After OPTIMIZE (no ZORDER):", after_opt)
print("After ZORDER:", after_z)

# -----------------------------
# 7) Range predicate scenario (ZORDER usually helps)
# -----------------------------
q_range = f"""
SELECT * FROM {TABLE}
WHERE order_date BETWEEN date('2026-01-03') AND date('2026-01-07')
  AND customer_id BETWEEN 100 AND 120
"""
files_read(q_range, "E) customer_id range filter AFTER ZORDER(customer_id)")

# -----------------------------
# 8) Scenario where ZORDER may not help much: random/high-cardinality key
# We'll create a pseudo-random column and test equality filter.
# -----------------------------
spark.sql(f"ALTER TABLE {TABLE} ADD COLUMNS (rand_key STRING)")

# Fill rand_key with a hash (UUID-like distribution)
spark.sql(f"""
UPDATE {TABLE}
SET rand_key = sha2(order_id, 256)
WHERE rand_key IS NULL
""")

some_key = spark.sql(f"SELECT rand_key FROM {TABLE} LIMIT 1").collect()[0][0]

q_rand = f"""
SELECT * FROM {TABLE}
WHERE order_date BETWEEN date('2026-01-03') AND date('2026-01-07')
  AND rand_key = '{some_key}'
"""
rand_before = files_read(q_rand, "F) rand_key filter BEFORE ZORDER(rand_key)")

# ZORDER on random keys is often expensive with limited benefit (demo once)
spark.sql(f"""
OPTIMIZE {TABLE}
WHERE order_date BETWEEN date('2026-01-03') AND date('2026-01-07')
ZORDER BY (rand_key)
""")

rand_after = files_read(q_rand, "G) rand_key filter AFTER ZORDER(rand_key)")

print("\nSummary for random key filter:")
print("Before ZORDER(rand_key):", rand_before)
print("After  ZORDER(rand_key):", rand_after)

# -----------------------------
# 9) Student takeaways (short and clear)
# -----------------------------
print("\n============================================================")
print("STUDENT TAKEAWAYS:")
print("1) OPTIMIZE fixes small files (fewer files), improving general reads.")
print("2) ZORDER clusters chosen columns inside files, improving data skipping for selective queries.")
print("3) Use ZORDER on columns frequently used in WHERE/JOIN (non-partition columns).")
print("4) Avoid ZORDER on random/high-cardinality keys unless you have a strong reason.")
print("5) In real systems, run OPTIMIZE/ZORDER periodically and usually only on hot partitions.")
print("============================================================\n")
