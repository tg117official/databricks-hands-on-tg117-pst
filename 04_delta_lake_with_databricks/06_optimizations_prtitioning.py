# ============================================================
# DELTA PARTITIONING COMPARISON (Single Script)
#
# Goal:
# Compare 3 tables side-by-side by running the SAME queries and showing
# how many data files Spark reads (proxy for pruning effectiveness).
#
# Tables:
# 1) Non-partitioned
# 2) Partitioned by (order_date)
# 3) Nested / multi-column partitioned by (order_date, city)
#
# What you will SEE:
# - Date filter: partitioned tables should read fewer files than non-partitioned
# - Date + city filter: nested partition should read the fewest files
# - City-only filter: may not help much when first partition column is order_date
#
# Run in Databricks Notebook (PySpark + SQL).
# ============================================================

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# -----------------------------
# 0) Names + locations
# -----------------------------
T_NONP = "cmp_orders_nonpart"
T_P1   = "cmp_orders_part_date"
T_P2   = "cmp_orders_part_date_city"

LOC_NONP = "/tmp/delta_partition_cmp/nonpart"
LOC_P1   = "/tmp/delta_partition_cmp/part_date"
LOC_P2   = "/tmp/delta_partition_cmp/part_date_city"

# -----------------------------
# 1) Helper: count files read
# -----------------------------
def files_read(table_name, sql_where, label):
    """
    Runs: SELECT * FROM table_name <sql_where>
    Prints number of input files Spark planned to read.
    """
    q = f"SELECT * FROM {table_name} {sql_where}"
    df = spark.sql(q)
    df.count()  # force execution
    n = len(df.inputFiles())
    print(f"{label:<22} | {table_name:<26} | files read = {n}")
    return n

def compare_all(sql_where, label):
    print(f"\n=== {label} ===")
    n1 = files_read(T_NONP, sql_where, "NON-PARTITIONED")
    n2 = files_read(T_P1,   sql_where, "PARTITIONED(date)")
    n3 = files_read(T_P2,   sql_where, "NESTED(date,city)")
    print(f"Result: NONP={n1}, P(date)={n2}, P(date,city)={n3}")

# -----------------------------
# 2) Clean start
# -----------------------------
for t in [T_NONP, T_P1, T_P2]:
    spark.sql(f"DROP TABLE IF EXISTS {t}")

# -----------------------------
# 3) Create tables
# -----------------------------
# Non-partitioned
spark.sql(f"""
CREATE TABLE {T_NONP} (
  order_id     STRING,
  customer_id  INT,
  order_date   DATE,
  city         STRING,
  amount       DOUBLE
)
USING DELTA
LOCATION '{LOC_NONP}'
""")

# Partitioned by date
spark.sql(f"""
CREATE TABLE {T_P1} (
  order_id     STRING,
  customer_id  INT,
  order_date   DATE,
  city         STRING,
  amount       DOUBLE
)
USING DELTA
PARTITIONED BY (order_date)
LOCATION '{LOC_P1}'
""")

# Nested / multi-column partitioned by (date, city)
spark.sql(f"""
CREATE TABLE {T_P2} (
  order_id     STRING,
  customer_id  INT,
  order_date   DATE,
  city         STRING,
  amount       DOUBLE
)
USING DELTA
PARTITIONED BY (order_date, city)
LOCATION '{LOC_P2}'
""")

# -----------------------------
# 4) Load the SAME data into all 3 tables (important for fair comparison)
# We insert in many small batches to create many small files.
# -----------------------------
# 10 days, 6 micro-batches/day, 800 rows each micro-batch
for day in range(1, 11):
    d = f"2026-01-{day:02d}"
    for b in range(6):
        insert_sql = f"""
        SELECT
          concat('o_', '{d}', '_', cast({b} as string), '_', cast(id as string)) AS order_id,
          cast(id % 2000 as int) AS customer_id,
          date('{d}') AS order_date,
          case when (id % 5)=0 then 'Pune'
               when (id % 5)=1 then 'Mumbai'
               when (id % 5)=2 then 'Nagpur'
               when (id % 5)=3 then 'Nashik'
               else 'Thane' end AS city,
          cast((id % 1000) * 1.0 as double) AS amount
        FROM range(0, 800)
        """
        spark.sql(f"INSERT INTO {T_NONP} {insert_sql}")
        spark.sql(f"INSERT INTO {T_P1}   {insert_sql}")
        spark.sql(f"INSERT INTO {T_P2}   {insert_sql}")

print("Data loaded into all 3 tables.")

# (Optional) Confirm row counts match
print("\nRow counts (should be equal):")
spark.sql(f"SELECT '{T_NONP}' AS table, COUNT(*) AS rows FROM {T_NONP}").show()
spark.sql(f"SELECT '{T_P1}'   AS table, COUNT(*) AS rows FROM {T_P1}").show()
spark.sql(f"SELECT '{T_P2}'   AS table, COUNT(*) AS rows FROM {T_P2}").show()

# -----------------------------
# 5) Run the SAME queries on each table and compare "files read"
# -----------------------------

# Q1) Date equality filter (best for partition pruning on order_date)
compare_all(
    "WHERE order_date = date('2026-01-05')",
    "Q1) Filter by one day (order_date = 2026-01-05)"
)

# Q2) Date range filter
compare_all(
    "WHERE order_date BETWEEN date('2026-01-03') AND date('2026-01-07')",
    "Q2) Filter by date range (2026-01-03 to 2026-01-07)"
)

# Q3) Date + city filter (nested partitioning should shine here)
compare_all(
    "WHERE order_date = date('2026-01-05') AND city = 'Pune'",
    "Q3) Filter by date + city (2026-01-05 AND Pune)"
)

# Q4) City-only filter (often NOT great when first partition column is order_date)
compare_all(
    "WHERE city = 'Pune'",
    "Q4) Filter by city only (city = Pune)"
)

# -----------------------------
# 6) Student takeaways
# -----------------------------
print("\n============================================================")
print("STUDENT TAKEAWAYS:")
print("1) Partition pruning is strongest when your WHERE clause includes the partition column(s).")
print("2) PARTITIONED BY (order_date) helps a lot for date filters and date ranges.")
print("3) PARTITIONED BY (order_date, city) helps most when queries filter by BOTH date AND city.")
print("4) City-only filters may still read many files because order_date is the first partition level.")
print("5) Avoid partitioning on high-cardinality columns (order_id/customer_id) -> too many partitions.")
print("============================================================\n")
