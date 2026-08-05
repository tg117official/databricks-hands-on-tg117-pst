# Delta Data Skipping and Partitioned Tables

## Session Overview

A Delta table can return correct results but still scan more data than necessary. Two important techniques help Azure Databricks reduce the amount of data considered during a query:

- **Data skipping** uses statistics recorded for individual data files.
- **Partition pruning** uses partition-column filters to avoid complete partitions.

This session first builds the data-skipping foundation and then explores partitioned Delta tables in detail.

The practical work uses external Delta tables stored in Azure Data Lake Storage Gen2.

---

# Learning Outcomes

By the end of this session, you should be able to:

- Explain why file layout affects query performance.
- Explain what file-level statistics Delta records.
- Inspect file-level statistics from the Delta transaction log.
- Differentiate data skipping from partition pruning.
- Create external partitioned Delta tables.
- Write data using static and dynamic partition values.
- Append a DataFrame to an existing partitioned table.
- Create and load a multi-level partitioned table.
- Combine static and dynamic partition values in one write.
- Inspect partition metadata and physical storage folders.
- Select suitable partition columns.
- Recognize over-partitioning, skew, and small-file risks.
- Choose whether a table should remain unpartitioned.

---

# Session Flow

```text
Understand why file layout affects query performance
        ↓
Create a fresh catalog and schema
        ↓
Create a clear sales dataset
        ↓
Write an unpartitioned table in separate batches
        ↓
Inspect Delta file-level statistics
        ↓
Understand data skipping
        ↓
Create a table partitioned by year
        ↓
Write to a selected static partition
        ↓
Dynamically load source rows into partitions
        ↓
Append a DataFrame to the partitioned table
        ↓
Inspect partition metadata and folders
        ↓
Understand partition pruning
        ↓
Create a year-and-month partitioned table
        ↓
Load all partition values dynamically
        ↓
Write to one fully static partition
        ↓
Combine a static year with dynamic months
        ↓
Compare full, partial, and non-partition filters
        ↓
Select suitable partition columns
        ↓
Discuss partitioning scenarios and risks
```

---

# 1. Environment and Storage Details

The exercises use the following objects:

| Object | Value |
|---|---|
| ADLS container | `data` |
| Storage account | `demodb117` |
| Catalog | `delta_partitioning_lab` |
| Schema | `sales_demo` |
| Root storage path | `abfss://data@demodb117.dfs.core.windows.net/databricks_training/delta_partitioning_session` |

Each external table uses a separate directory below the root path.

```text
abfss://data@demodb117.dfs.core.windows.net/
└── databricks_training/
    └── delta_partitioning_session/
        ├── sales_unpartitioned/
        ├── sales_partitioned_year/
        └── sales_partitioned_year_month/
```

## Required access

Before running the notebook, confirm that:

- Unity Catalog is enabled.
- The ADLS path is covered by a registered Unity Catalog external location.
- You have `CREATE EXTERNAL TABLE` access on that external location.
- You have permission to create a catalog, schema, and tables.
- The compute can access Unity Catalog external tables.

A `LOCATION` clause creates an external table. Dropping the table removes Unity Catalog metadata but does not automatically delete the underlying ADLS files.

---

# 2. Protected Cleanup for a Fresh Run

The following cell removes only the dedicated session directory. It does not delete the container root.

```python
# Remove catalog metadata from an earlier run.
spark.sql(
    "DROP CATALOG IF EXISTS delta_partitioning_lab CASCADE"
)

# External-table data is not removed by DROP TABLE or DROP CATALOG.
# This command removes only the dedicated directory used in this session.
dbutils.fs.rm(
    "abfss://data@demodb117.dfs.core.windows.net/"
    "databricks_training/delta_partitioning_session",
    True
)

print("Earlier metadata and session files were removed.")
```

> Run the cleanup only when you are certain that the path is dedicated to this session.

---

# 3. Create a Fresh Catalog and Schema

```sql
CREATE CATALOG IF NOT EXISTS delta_partitioning_lab;

CREATE SCHEMA IF NOT EXISTS delta_partitioning_lab.sales_demo;

USE CATALOG delta_partitioning_lab;
USE SCHEMA sales_demo;
```

Confirm the current namespace:

```sql
SELECT
    current_catalog() AS current_catalog,
    current_schema() AS current_schema;
```

Expected values:

```text
current_catalog = delta_partitioning_lab
current_schema  = sales_demo
```

---

# 4. Create the Sales Source Dataset

The dataset contains 24 orders distributed across:

- Two years: `2025` and `2026`
- Two months in each year: January and February
- Four ingestion batches
- Four regions
- Multiple product categories and order statuses

The customer ranges are deliberately separated by batch:

```text
Batch 1 → customer_id 101 to 106
Batch 2 → customer_id 201 to 206
Batch 3 → customer_id 301 to 306
Batch 4 → customer_id 401 to 406
```

This distribution makes the file statistics easy to discuss.

```sql
CREATE OR REPLACE TEMP VIEW sales_source AS
SELECT
    CAST(order_id AS BIGINT)              AS order_id,
    CAST(customer_id AS INT)              AS customer_id,
    CAST(order_date AS DATE)              AS order_date,
    CAST(order_year AS INT)               AS order_year,
    CAST(order_month AS INT)              AS order_month,
    CAST(region AS STRING)                AS region,
    CAST(product_category AS STRING)      AS product_category,
    CAST(order_status AS STRING)           AS order_status,
    CAST(order_amount AS DECIMAL(12, 2))  AS order_amount,
    CAST(ingestion_batch AS INT)          AS ingestion_batch
FROM VALUES
    (1001, 101, DATE '2025-01-05', 2025, 1, 'NORTH', 'ELECTRONICS', 'DELIVERED', 1200.00, 1),
    (1002, 102, DATE '2025-01-08', 2025, 1, 'SOUTH', 'BOOKS',       'SHIPPED',    450.00, 1),
    (1003, 103, DATE '2025-01-12', 2025, 1, 'EAST',  'FASHION',     'PLACED',    1800.00, 1),
    (1004, 104, DATE '2025-01-18', 2025, 1, 'WEST',  'HOME',        'DELIVERED', 3200.00, 1),
    (1005, 105, DATE '2025-01-22', 2025, 1, 'NORTH', 'ELECTRONICS', 'CANCELLED',  950.00, 1),
    (1006, 106, DATE '2025-01-28', 2025, 1, 'SOUTH', 'FASHION',     'DELIVERED', 2100.00, 1),

    (2001, 201, DATE '2025-02-03', 2025, 2, 'EAST',  'BOOKS',       'DELIVERED',  699.00, 2),
    (2002, 202, DATE '2025-02-07', 2025, 2, 'WEST',  'ELECTRONICS', 'SHIPPED',   5400.00, 2),
    (2003, 203, DATE '2025-02-11', 2025, 2, 'NORTH', 'HOME',        'PLACED',    2750.00, 2),
    (2004, 204, DATE '2025-02-16', 2025, 2, 'SOUTH', 'FASHION',     'DELIVERED', 1650.00, 2),
    (2005, 205, DATE '2025-02-21', 2025, 2, 'EAST',  'ELECTRONICS', 'CANCELLED', 8900.00, 2),
    (2006, 206, DATE '2025-02-26', 2025, 2, 'WEST',  'BOOKS',       'SHIPPED',    799.00, 2),

    (3001, 301, DATE '2026-01-04', 2026, 1, 'NORTH', 'HOME',        'DELIVERED', 4100.00, 3),
    (3002, 302, DATE '2026-01-09', 2026, 1, 'SOUTH', 'ELECTRONICS', 'PLACED',   12750.00, 3),
    (3003, 303, DATE '2026-01-13', 2026, 1, 'EAST',  'BOOKS',       'SHIPPED',    550.00, 3),
    (3004, 304, DATE '2026-01-17', 2026, 1, 'WEST',  'FASHION',     'DELIVERED', 2350.00, 3),
    (3005, 305, DATE '2026-01-23', 2026, 1, 'NORTH', 'ELECTRONICS', 'CANCELLED', 7600.00, 3),
    (3006, 306, DATE '2026-01-29', 2026, 1, 'SOUTH', 'HOME',        'SHIPPED',   3300.00, 3),

    (4001, 401, DATE '2026-02-02', 2026, 2, 'EAST',  'FASHION',     'DELIVERED', 1450.00, 4),
    (4002, 402, DATE '2026-02-06', 2026, 2, 'WEST',  'BOOKS',       'PLACED',     920.00, 4),
    (4003, 403, DATE '2026-02-10', 2026, 2, 'NORTH', 'ELECTRONICS', 'SHIPPED',   6800.00, 4),
    (4004, 404, DATE '2026-02-15', 2026, 2, 'SOUTH', 'HOME',        'DELIVERED', 2950.00, 4),
    (4005, 405, DATE '2026-02-20', 2026, 2, 'EAST',  'ELECTRONICS', 'CANCELLED', 9990.00, 4),
    (4006, 406, DATE '2026-02-25', 2026, 2, 'WEST',  'FASHION',     'SHIPPED',   1850.00, 4)
AS source_data
(
    order_id,
    customer_id,
    order_date,
    order_year,
    order_month,
    region,
    product_category,
    order_status,
    order_amount,
    ingestion_batch
);
```

Validate the source:

```sql
SELECT
    ingestion_batch,
    order_year,
    order_month,
    COUNT(*) AS record_count,
    MIN(customer_id) AS minimum_customer_id,
    MAX(customer_id) AS maximum_customer_id
FROM sales_source
GROUP BY
    ingestion_batch,
    order_year,
    order_month
ORDER BY ingestion_batch;
```

Expected result:

| ingestion_batch | order_year | order_month | record_count | minimum_customer_id | maximum_customer_id |
|---:|---:|---:|---:|---:|---:|
| 1 | 2025 | 1 | 6 | 101 | 106 |
| 2 | 2025 | 2 | 6 | 201 | 206 |
| 3 | 2026 | 1 | 6 | 301 | 306 |
| 4 | 2026 | 2 | 6 | 401 | 406 |

---

# 5. Why File Layout Matters

Suppose a query requests customer `204`:

```sql
SELECT *
FROM sales_source
WHERE customer_id = 204;
```

Only one record matches, but a storage engine must determine which files might contain that customer.

```text
Poorly separated file ranges
    → Many files might contain customer 204
    → More files must be considered

Clearly separated file ranges
    → Files outside the customer range can be skipped
    → Less data is read
```

---

# 6. Create an Unpartitioned Baseline Table

```sql
CREATE TABLE delta_partitioning_lab.sales_demo.sales_unpartitioned
(
    order_id BIGINT,
    customer_id INT,
    order_date DATE,
    order_year INT,
    order_month INT,
    region STRING,
    product_category STRING,
    order_status STRING,
    order_amount DECIMAL(12, 2),
    ingestion_batch INT
)
USING DELTA
LOCATION 'abfss://data@demodb117.dfs.core.windows.net/databricks_training/delta_partitioning_session/sales_unpartitioned';
```

Write each ingestion batch as a separate transaction:

```sql
INSERT INTO delta_partitioning_lab.sales_demo.sales_unpartitioned BY NAME
SELECT * FROM sales_source WHERE ingestion_batch = 1;

INSERT INTO delta_partitioning_lab.sales_demo.sales_unpartitioned BY NAME
SELECT * FROM sales_source WHERE ingestion_batch = 2;

INSERT INTO delta_partitioning_lab.sales_demo.sales_unpartitioned BY NAME
SELECT * FROM sales_source WHERE ingestion_batch = 3;

INSERT INTO delta_partitioning_lab.sales_demo.sales_unpartitioned BY NAME
SELECT * FROM sales_source WHERE ingestion_batch = 4;
```

Validate the table:

```sql
SELECT
    COUNT(*) AS total_records,
    COUNT(DISTINCT ingestion_batch) AS batch_count,
    MIN(customer_id) AS minimum_customer_id,
    MAX(customer_id) AS maximum_customer_id
FROM delta_partitioning_lab.sales_demo.sales_unpartitioned;
```

Expected result:

```text
total_records       = 24
batch_count         = 4
minimum_customer_id = 101
maximum_customer_id = 406
```

Inspect table details and history:

```sql
DESCRIBE DETAIL delta_partitioning_lab.sales_demo.sales_unpartitioned;
```

```sql
DESCRIBE HISTORY delta_partitioning_lab.sales_demo.sales_unpartitioned;
```

Because each batch was written separately, the history should contain separate write operations. The exact number of physical files can vary with the runtime, compute type, and optimized-write behaviour.

---

# 7. Inspect the Physical Table Directory

```python
display(
    dbutils.fs.ls(
        "abfss://data@demodb117.dfs.core.windows.net/"
        "databricks_training/delta_partitioning_session/"
        "sales_unpartitioned"
    )
)
```

You should see:

- One `_delta_log` directory
- One or more Parquet data files

Do not manually add, modify, move, or delete files inside a Delta table directory.

---

# 8. What Is Data Skipping?

Delta Lake records statistics about individual data files. These statistics can include:

- Number of records
- Minimum value for a column
- Maximum value for a column
- Number of null values

When a query contains a filter, Databricks compares the filter with these statistics.

Example:

```text
File A: customer_id 101 to 106
File B: customer_id 201 to 206
File C: customer_id 301 to 306
File D: customer_id 401 to 406
```

For this query:

```sql
SELECT *
FROM delta_partitioning_lab.sales_demo.sales_unpartitioned
WHERE customer_id = 204;
```

Databricks can reason as follows:

```text
File A → 204 is outside 101–106 → Skip
File B → 204 is inside 201–206  → Read
File C → 204 is outside 301–306 → Skip
File D → 204 is outside 401–406 → Skip
```

The query still returns the correct record. Data skipping reduces the number of irrelevant files that need to be read.

## Data skipping is not a database index

```text
Database index
    → Separate lookup structure

Delta data skipping
    → Uses statistics stored with Delta file actions
    → Skips files that cannot contain matching values
```

---

# 9. Inspect File-Level Statistics in `_delta_log`

The statistics are maintained internally, but they can be inspected from Delta JSON commit files.

```python
from pyspark.sql import functions as F

log_df = spark.read.json(
    "abfss://data@demodb117.dfs.core.windows.net/"
    "databricks_training/delta_partitioning_session/"
    "sales_unpartitioned/_delta_log/*.json"
)

file_stats_df = (
    log_df
    .where(F.col("add.path").isNotNull())
    .select(
        F.col("add.path").alias("data_file"),
        F.get_json_object(F.col("add.stats"), "$.numRecords")
            .cast("long")
            .alias("num_records"),
        F.get_json_object(F.col("add.stats"), "$.minValues.customer_id")
            .cast("int")
            .alias("minimum_customer_id"),
        F.get_json_object(F.col("add.stats"), "$.maxValues.customer_id")
            .cast("int")
            .alias("maximum_customer_id"),
        F.get_json_object(F.col("add.stats"), "$.minValues.order_date")
            .alias("minimum_order_date"),
        F.get_json_object(F.col("add.stats"), "$.maxValues.order_date")
            .alias("maximum_order_date")
    )
)

display(
    file_stats_df.orderBy(
        "minimum_customer_id",
        "data_file"
    )
)
```

## What to observe

Look for files whose customer ranges include:

```text
101–106
201–206
301–306
401–406
```

The exact number of rows in the statistics output might be greater than four because one transaction can write multiple files.

## Query to test data skipping

```sql
SELECT
    order_id,
    customer_id,
    order_date,
    region,
    product_category,
    order_amount
FROM delta_partitioning_lab.sales_demo.sales_unpartitioned
WHERE customer_id = 204;
```

Expected record:

| order_id | customer_id | order_date | region | product_category | order_amount |
|---:|---:|---|---|---|---:|
| 2004 | 204 | 2025-02-16 | SOUTH | FASHION | 1650.00 |

Open the query profile and observe the scan metrics. On a very small table, the performance difference may not be noticeable, but the file-level ranges show why skipping is possible.

---

# 10. Data Skipping and Statistics Coverage

For Unity Catalog external Delta tables, statistics are collected automatically for a limited set of schema columns by default. This table has only ten columns, so the columns used in the session are within the normal coverage.

You can explicitly select statistics columns when needed:

```sql
ALTER TABLE delta_partitioning_lab.sales_demo.sales_unpartitioned
SET TBLPROPERTIES
(
    'delta.dataSkippingStatsColumns' =
    'order_id,customer_id,order_date,order_year,order_month,region'
);
```

Changing this property affects statistics collection for future writes. Recompute statistics for existing files on supported runtimes:

```sql
ANALYZE TABLE delta_partitioning_lab.sales_demo.sales_unpartitioned
COMPUTE DELTA STATISTICS;
```

For this session, the property change is shown to explain the configuration. It is not required for the ten-column table created above.

---

# 11. What Is Partitioning?

Partitioning groups rows according to the values of selected columns.

If a table is partitioned by `order_year`, its logical layout contains groups such as:

```text
order_year=2025
order_year=2026
```

If a query filters only `2025`, Databricks can avoid the `2026` partition.

```text
WHERE order_year = 2025
        ↓
Read the 2025 partition
Skip the 2026 partition
```

This is called **partition pruning**.

---

# 12. Data Skipping Versus Partition Pruning

| Data skipping | Partition pruning |
|---|---|
| Works at the data-file level | Works at the partition level |
| Uses file-level statistics | Uses partition-column values |
| Can work on non-partition columns | Requires a partition-column filter |
| Skips files that cannot match | Skips complete partitions that cannot match |

A query can benefit from both techniques.

```text
Partition pruning
    → Select the relevant partitions

Data skipping
    → Skip irrelevant files inside those partitions
```

---

# 13. Important Delta Partition Behaviour

Delta automatically tracks its partitions when data is written or removed.

Do not manually register Delta partitions using commands such as:

```text
ALTER TABLE ... ADD PARTITION
ALTER TABLE ... DROP PARTITION
MSCK REPAIR TABLE
```

Those commands are used in other table formats and partition-discovery scenarios. They are not the normal way to manage Delta partitions.

For Delta tables, writing a row with a new partition value automatically creates and tracks that partition.

---

# 14. Create a Table Partitioned by Year

```sql
CREATE TABLE delta_partitioning_lab.sales_demo.sales_partitioned_year
(
    order_id BIGINT,
    customer_id INT,
    order_date DATE,
    order_year INT,
    order_month INT,
    region STRING,
    product_category STRING,
    order_status STRING,
    order_amount DECIMAL(12, 2),
    ingestion_batch INT
)
USING DELTA
PARTITIONED BY (order_year)
LOCATION 'abfss://data@demodb117.dfs.core.windows.net/databricks_training/delta_partitioning_session/sales_partitioned_year';
```

Inspect the partition configuration:

```sql
DESCRIBE DETAIL delta_partitioning_lab.sales_demo.sales_partitioned_year;
```

Look for:

```text
partitionColumns = [order_year]
```

---

# 15. Method 1 — Write to a Selected Static Partition

A static partition value is written directly in the `PARTITION` clause.

The following command writes batches 1 and 2 to `order_year = 2025`.

```sql
INSERT INTO delta_partitioning_lab.sales_demo.sales_partitioned_year
PARTITION (order_year = 2025)
(
    order_id,
    customer_id,
    order_date,
    order_month,
    region,
    product_category,
    order_status,
    order_amount,
    ingestion_batch
)
SELECT
    order_id,
    customer_id,
    order_date,
    order_month,
    region,
    product_category,
    order_status,
    order_amount,
    ingestion_batch
FROM sales_source
WHERE ingestion_batch IN (1, 2);
```

Notice that `order_year` is not selected from the source. Its value is fixed by:

```text
PARTITION (order_year = 2025)
```

Validate the result:

```sql
SELECT
    order_year,
    COUNT(*) AS record_count
FROM delta_partitioning_lab.sales_demo.sales_partitioned_year
GROUP BY order_year
ORDER BY order_year;
```

Expected result at this stage:

| order_year | record_count |
|---:|---:|
| 2025 | 12 |

## Static-partition safety point

A static partition value overrides the need to read that partition column from the selected data. Confirm that the source rows genuinely belong to the selected partition before running the write.

---

# 16. Method 2 — Dynamic Partition Loading with SQL

In a dynamic load, partition values come from the source records.

```sql
INSERT INTO delta_partitioning_lab.sales_demo.sales_partitioned_year BY NAME
SELECT *
FROM sales_source
WHERE ingestion_batch = 3;
```

Batch 3 contains:

```text
order_year = 2026
```

Delta automatically routes these rows to the `2026` partition.

Validate the result:

```sql
SELECT
    order_year,
    COUNT(*) AS record_count
FROM delta_partitioning_lab.sales_demo.sales_partitioned_year
GROUP BY order_year
ORDER BY order_year;
```

Expected result at this stage:

| order_year | record_count |
|---:|---:|
| 2025 | 12 |
| 2026 | 6 |

## Why `BY NAME` is useful

Partition columns can appear in a different physical position from the order used while defining the table. `BY NAME` matches source and target columns using their names instead of relying only on position.

---

# 17. Method 3 — Append a DataFrame to the Partitioned Table

Create a DataFrame for ingestion batch 4:

```python
batch_4_df = (
    spark.table("sales_source")
    .where("ingestion_batch = 4")
)

display(batch_4_df.orderBy("order_id"))
```

Append it through the registered table:

```python
(
    batch_4_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(
        "delta_partitioning_lab.sales_demo.sales_partitioned_year"
    )
)
```

The incoming rows contain `order_year = 2026`, so Delta routes them to the `2026` partition.

Validate the final year counts:

```sql
SELECT
    order_year,
    COUNT(*) AS record_count,
    MIN(order_id) AS minimum_order_id,
    MAX(order_id) AS maximum_order_id
FROM delta_partitioning_lab.sales_demo.sales_partitioned_year
GROUP BY order_year
ORDER BY order_year;
```

Expected result:

| order_year | record_count | minimum_order_id | maximum_order_id |
|---:|---:|---:|---:|
| 2025 | 12 | 1001 | 2006 |
| 2026 | 12 | 3001 | 4006 |

Total records:

```sql
SELECT COUNT(*) AS total_records
FROM delta_partitioning_lab.sales_demo.sales_partitioned_year;
```

Expected:

```text
total_records = 24
```

---

# 18. Comparing the Three Write Methods

| Method | Partition value comes from | Suitable use |
|---|---|---|
| Static SQL partition insert | The `PARTITION` clause | A known, controlled target partition |
| Dynamic SQL load | Source columns | Multi-partition batch ingestion |
| DataFrame append | DataFrame partition-column values | PySpark ingestion pipelines |

All three methods write through the registered Delta table and update the Delta transaction log.

## Position-based `insertInto` caution

`DataFrameWriter.insertInto()` is position-based. A source DataFrame with the wrong column order can place values into incorrect target columns or fail with a type error.

Prefer one of the following:

- Select columns in the exact target order before `insertInto()`.
- Use `saveAsTable()` for the append demonstrated above.
- Use SQL `INSERT ... BY NAME`.

---

# 19. Inspect the Partitions

```sql
SHOW PARTITIONS delta_partitioning_lab.sales_demo.sales_partitioned_year;
```

Expected partition values:

```text
order_year=2025
order_year=2026
```

Inspect the physical root directory:

```python
display(
    dbutils.fs.ls(
        "abfss://data@demodb117.dfs.core.windows.net/"
        "databricks_training/delta_partitioning_session/"
        "sales_partitioned_year"
    )
)
```

You should see partition directories representing the year values, along with `_delta_log`.

The physical folder structure is useful for observation, but always read and write Delta data through the Delta table or supported Delta APIs.

---

# 20. Demonstrate Partition Pruning

## Query A — Filter using the partition column

```sql
SELECT
    COUNT(*) AS record_count,
    SUM(order_amount) AS total_amount
FROM delta_partitioning_lab.sales_demo.sales_partitioned_year
WHERE order_year = 2025;
```

Explain the physical plan:

```sql
EXPLAIN FORMATTED
SELECT
    COUNT(*) AS record_count,
    SUM(order_amount) AS total_amount
FROM delta_partitioning_lab.sales_demo.sales_partitioned_year
WHERE order_year = 2025;
```

Look for a partition filter involving:

```text
order_year = 2025
```

## Query B — Filter only a non-partition column

```sql
SELECT *
FROM delta_partitioning_lab.sales_demo.sales_partitioned_year
WHERE customer_id = 204;
```

This query cannot prune a year using the filter alone because `customer_id` is not a partition column. File-level data skipping may still help.

## Query C — Combine partition pruning and data skipping

```sql
SELECT *
FROM delta_partitioning_lab.sales_demo.sales_partitioned_year
WHERE order_year = 2025
  AND customer_id = 204;
```

Possible behaviour:

```text
order_year = 2025
    → Partition pruning

customer_id = 204
    → File-level data skipping inside the selected partition
```

---

# 21. Multi-Level Partitioning

A table can be partitioned by more than one top-level column.

For this session, use:

```text
order_year
order_month
```

The logical layout becomes:

```text
order_year=2025/
    order_month=1/
    order_month=2/

order_year=2026/
    order_month=1/
    order_month=2/
```

The term **multi-level partitioning** is clearer than nested partitioning here.

A nested struct field such as `customer.address.city` cannot be used directly as a Delta partition column. Partition columns must be top-level columns.

---

# 22. Create the Year-and-Month Partitioned Table

```sql
CREATE TABLE delta_partitioning_lab.sales_demo.sales_partitioned_year_month
(
    order_id BIGINT,
    customer_id INT,
    order_date DATE,
    order_year INT,
    order_month INT,
    region STRING,
    product_category STRING,
    order_status STRING,
    order_amount DECIMAL(12, 2),
    ingestion_batch INT
)
USING DELTA
PARTITIONED BY (order_year, order_month)
LOCATION 'abfss://data@demodb117.dfs.core.windows.net/databricks_training/delta_partitioning_session/sales_partitioned_year_month';
```

Inspect its partition columns:

```sql
DESCRIBE DETAIL delta_partitioning_lab.sales_demo.sales_partitioned_year_month;
```

Expected partition configuration:

```text
partitionColumns = [order_year, order_month]
```

---

# 23. Fully Dynamic Load into Multiple Partition Levels

Load all 24 source records dynamically:

```sql
INSERT INTO delta_partitioning_lab.sales_demo.sales_partitioned_year_month BY NAME
SELECT *
FROM sales_source;
```

Delta reads both partition values from each source row and routes it to the correct year-and-month partition.

```sql
SELECT
    order_year,
    order_month,
    COUNT(*) AS record_count
FROM delta_partitioning_lab.sales_demo.sales_partitioned_year_month
GROUP BY
    order_year,
    order_month
ORDER BY
    order_year,
    order_month;
```

Expected result:

| order_year | order_month | record_count |
|---:|---:|---:|
| 2025 | 1 | 6 |
| 2025 | 2 | 6 |
| 2026 | 1 | 6 |
| 2026 | 2 | 6 |

---

# 24. Write to One Fully Static Year-and-Month Partition

The following statement fixes both partition values:

```sql
INSERT INTO delta_partitioning_lab.sales_demo.sales_partitioned_year_month
PARTITION
(
    order_year = 2026,
    order_month = 3
)
(
    order_id,
    customer_id,
    order_date,
    region,
    product_category,
    order_status,
    order_amount,
    ingestion_batch
)
VALUES
    (5001, 501, DATE '2026-03-05', 'NORTH', 'ELECTRONICS', 'DELIVERED', 7200.00, 5),
    (5002, 502, DATE '2026-03-18', 'SOUTH', 'HOME',        'SHIPPED',   3600.00, 5);
```

The selected values do not include `order_year` or `order_month` because both are fixed in the partition clause.

Validate the new partition:

```sql
SELECT *
FROM delta_partitioning_lab.sales_demo.sales_partitioned_year_month
WHERE order_year = 2026
  AND order_month = 3
ORDER BY order_id;
```

Expected records:

| order_id | customer_id | order_date | order_year | order_month | region | product_category | order_status | order_amount | ingestion_batch |
|---:|---:|---|---:|---:|---|---|---|---:|---:|
| 5001 | 501 | 2026-03-05 | 2026 | 3 | NORTH | ELECTRONICS | DELIVERED | 7200.00 | 5 |
| 5002 | 502 | 2026-03-18 | 2026 | 3 | SOUTH | HOME | SHIPPED | 3600.00 | 5 |

---

# 25. Combine a Static Year with Dynamic Months

This example fixes the year as `2025`, while the month comes from each source row.

Create a source containing April and May records:

```sql
CREATE OR REPLACE TEMP VIEW partial_static_source AS
SELECT
    CAST(order_id AS BIGINT)              AS order_id,
    CAST(customer_id AS INT)              AS customer_id,
    CAST(order_date AS DATE)              AS order_date,
    CAST(order_month AS INT)              AS order_month,
    CAST(region AS STRING)                AS region,
    CAST(product_category AS STRING)      AS product_category,
    CAST(order_status AS STRING)           AS order_status,
    CAST(order_amount AS DECIMAL(12, 2))  AS order_amount,
    CAST(ingestion_batch AS INT)          AS ingestion_batch
FROM VALUES
    (6001, 601, DATE '2025-04-04', 4, 'EAST',  'BOOKS',       'DELIVERED',  850.00, 6),
    (6002, 602, DATE '2025-04-19', 4, 'WEST',  'ELECTRONICS', 'SHIPPED',   6400.00, 6),
    (6003, 603, DATE '2025-05-07', 5, 'NORTH', 'FASHION',     'PLACED',    1950.00, 6),
    (6004, 604, DATE '2025-05-23', 5, 'SOUTH', 'HOME',        'DELIVERED', 2850.00, 6)
AS source_data
(
    order_id,
    customer_id,
    order_date,
    order_month,
    region,
    product_category,
    order_status,
    order_amount,
    ingestion_batch
);
```

Insert the data:

```sql
INSERT INTO delta_partitioning_lab.sales_demo.sales_partitioned_year_month
PARTITION (order_year = 2025)
(
    order_id,
    customer_id,
    order_date,
    order_month,
    region,
    product_category,
    order_status,
    order_amount,
    ingestion_batch
)
SELECT
    order_id,
    customer_id,
    order_date,
    order_month,
    region,
    product_category,
    order_status,
    order_amount,
    ingestion_batch
FROM partial_static_source;
```

Interpretation:

```text
order_year = 2025
    → Static value supplied by the command

order_month = 4 or 5
    → Dynamic value supplied by each source row
```

Validate the new partitions:

```sql
SELECT
    order_year,
    order_month,
    COUNT(*) AS record_count,
    MIN(order_id) AS minimum_order_id,
    MAX(order_id) AS maximum_order_id
FROM delta_partitioning_lab.sales_demo.sales_partitioned_year_month
GROUP BY
    order_year,
    order_month
ORDER BY
    order_year,
    order_month;
```

Expected result:

| order_year | order_month | record_count | minimum_order_id | maximum_order_id |
|---:|---:|---:|---:|---:|
| 2025 | 1 | 6 | 1001 | 1006 |
| 2025 | 2 | 6 | 2001 | 2006 |
| 2025 | 4 | 2 | 6001 | 6002 |
| 2025 | 5 | 2 | 6003 | 6004 |
| 2026 | 1 | 6 | 3001 | 3006 |
| 2026 | 2 | 6 | 4001 | 4006 |
| 2026 | 3 | 2 | 5001 | 5002 |

Total records:

```sql
SELECT COUNT(*) AS total_records
FROM delta_partitioning_lab.sales_demo.sales_partitioned_year_month;
```

Expected:

```text
total_records = 30
```

---

# 26. Inspect Multi-Level Partitions

```sql
SHOW PARTITIONS delta_partitioning_lab.sales_demo.sales_partitioned_year_month;
```

Expected values include:

```text
order_year=2025/order_month=1
order_year=2025/order_month=2
order_year=2025/order_month=4
order_year=2025/order_month=5
order_year=2026/order_month=1
order_year=2026/order_month=2
order_year=2026/order_month=3
```

List the physical root:

```python
display(
    dbutils.fs.ls(
        "abfss://data@demodb117.dfs.core.windows.net/"
        "databricks_training/delta_partitioning_session/"
        "sales_partitioned_year_month"
    )
)
```

List the 2025 directory:

```python
display(
    dbutils.fs.ls(
        "abfss://data@demodb117.dfs.core.windows.net/"
        "databricks_training/delta_partitioning_session/"
        "sales_partitioned_year_month/order_year=2025"
    )
)
```

You should see month directories under the year directory.

---

# 27. Full, Partial, and Non-Partition Filters

## Full partition filter

Both partition columns are provided:

```sql
SELECT
    COUNT(*) AS record_count,
    SUM(order_amount) AS total_amount
FROM delta_partitioning_lab.sales_demo.sales_partitioned_year_month
WHERE order_year = 2025
  AND order_month = 2;
```

This can target one year-and-month partition.

## Partial partition filter

Only the first partition column is provided:

```sql
SELECT
    COUNT(*) AS record_count,
    SUM(order_amount) AS total_amount
FROM delta_partitioning_lab.sales_demo.sales_partitioned_year_month
WHERE order_year = 2025;
```

This can prune all `2026` partitions but still considers all matching 2025 month partitions.

## Filter only the second partition column

```sql
SELECT
    COUNT(*) AS record_count,
    SUM(order_amount) AS total_amount
FROM delta_partitioning_lab.sales_demo.sales_partitioned_year_month
WHERE order_month = 1;
```

This can select January partitions from both years.

## Non-partition filter

```sql
SELECT *
FROM delta_partitioning_lab.sales_demo.sales_partitioned_year_month
WHERE customer_id = 204;
```

The filter does not directly identify a year or month partition. Delta can still use file-level statistics.

## Combined partition and non-partition filter

```sql
SELECT *
FROM delta_partitioning_lab.sales_demo.sales_partitioned_year_month
WHERE order_year = 2025
  AND order_month = 2
  AND customer_id = 204;
```

This query can combine:

- Partition pruning using `order_year` and `order_month`
- Data skipping using `customer_id`

---

# 28. Selecting a Partition Column

Use the following questions before selecting a partition column:

```text
Is the column frequently used in filters?
        ↓
Does it have controlled cardinality?
        ↓
Will every partition contain substantial data?
        ↓
Is the distribution reasonably balanced?
        ↓
Will the choice remain useful as data grows?
```

## Characteristics of a useful partition column

- Frequently used in filters
- Low or controlled cardinality
- Produces sufficiently large partitions
- Produces reasonably balanced partitions
- Reflects stable query patterns

Possible candidates for very large tables:

```text
business_date
order_month
country
region
business_unit
```

The correct choice depends on table size, data distribution, and query behaviour.

## Risky partition columns

Avoid columns with extremely high cardinality:

```text
order_id
customer_id
email
transaction_id
exact event timestamp
```

These columns can create too many partitions and many small files.

---

# 29. Partitioning Scenarios

## Scenario 1 — Large monthly reporting table

```text
Data size          → Very large
Queries            → Almost always filter by month
Data per month     → Substantial
Distribution       → Reasonably balanced
```

Possible decision:

```text
Partition by order_month
```

## Scenario 2 — Customer transaction table

```text
Customers          → Millions
Queries            → Filter by customer_id
```

Poor decision:

```text
Partition by customer_id
```

Reason:

```text
Very high cardinality
Many tiny partitions
Metadata overhead
Small-file risk
```

Customer filtering is a better candidate for a modern clustering technique discussed in the next session.

## Scenario 3 — Global sales history

```text
Data               → Several years
Queries            → Usually filter by year and month
Monthly volume     → Large
```

Possible traditional choice:

```text
PARTITIONED BY (order_year, order_month)
```

Before using two partition levels, confirm that every month contains enough data.

## Scenario 4 — Regional data with skew

```text
WEST               → 80% of records
NORTH, SOUTH, EAST → 20% combined
```

Risk:

```text
Unbalanced partition sizes
One very large partition
Several small partitions
```

A low number of values does not automatically make a column a good partition key.

## Scenario 5 — Small lookup table

```text
Size               → A few gigabytes
Read pattern       → Often read completely
```

Decision:

```text
Do not partition
```

Partitioning adds complexity without a meaningful reduction in scanned data.

## Scenario 6 — High-frequency events

```text
Event timestamp    → Unique or nearly unique
```

Poor decision:

```text
Partition by exact event_timestamp
```

Possible traditional alternatives:

```text
Extract event_date
or
Extract event_hour only when every hour contains substantial data
```

---

# 30. Modern Databricks Guidance

Partitioning remains important for understanding existing data lakes and very large tables. However, it should not be applied automatically to every Delta table.

Current guidance emphasizes:

- Do not partition small tables.
- A table below approximately 1 TB normally should not be partitioned.
- Every partition should contain substantial data, commonly at least about 1 GB.
- Liquid clustering should be evaluated before partitioning new Delta tables.
- An ineffective partition design can require a complete rewrite to correct.

The tables in this notebook are intentionally small so the mechanics remain visible. Their size is not an example of when production partitioning is recommended.

---

# 31. Common Blind Spots

## Blind spot 1 — More partitions do not always mean faster queries

Too many partitions can produce:

- Small files
- Metadata overhead
- Slow writes
- Expensive maintenance

## Blind spot 2 — Low cardinality alone is not sufficient

A `region` column might contain only four values, but severe skew can make one partition much larger than the others.

## Blind spot 3 — Partitioning does not replace data skipping

Files inside a selected partition can still be skipped using file statistics.

## Blind spot 4 — Data skipping does not require partitioning

The unpartitioned baseline table can still use file-level statistics.

## Blind spot 5 — Static inserts can misclassify data

If source rows belong to 2026 but the command forces:

```text
PARTITION (order_year = 2025)
```

the table receives the static value supplied by the command. Validate static partition values before writing.

## Blind spot 6 — Dynamic loading is not dynamic overwrite

```text
Dynamic partition loading
    → Appends rows and routes them using source partition values

Dynamic partition overwrite
    → Replaces complete partitions touched by incoming data
```

Dynamic partition overwrite is a separate topic and can delete more data than expected if one row contains an incorrect partition value.

## Blind spot 7 — Do not depend on physical folders as the table contract

Delta uses the transaction log as the source of truth. Use supported Delta readers and writers instead of directly managing partition directories.

## Blind spot 8 — Query duration alone is not enough

A repeated query can be faster because of caching or warm compute. Compare:

- Partition filters
- Files read
- Files pruned or skipped
- Bytes scanned
- Query profile

---

# 32. Review Questions

1. What is the difference between data skipping and partition pruning?
2. Where are Delta file-level statistics maintained?
3. Why can customer `204` allow some unpartitioned files to be skipped?
4. What does a static partition value mean?
5. What does a dynamic partition load mean?
6. Why is `INSERT ... BY NAME` useful?
7. Why should `customer_id` usually not be a partition column?
8. What happens when a new partition value arrives in a Delta write?
9. Why should `ALTER TABLE ADD PARTITION` not be used for the Delta tables in this session?
10. What is the risk of partitioning a small table?
11. Can one query use both partition pruning and data skipping?
12. Why might `region` be a poor partition column even if it has only four values?

---

# 33. Review Answers

<details>
<summary>View answers</summary>

1. Data skipping skips individual files using file statistics. Partition pruning skips partitions using partition-column filters.
2. They are stored in Delta transaction-log file actions.
3. Files whose minimum and maximum customer IDs cannot include `204` can be skipped.
4. The command fixes the partition value instead of taking it from each source row.
5. Partition values come from source records, and Delta routes each row automatically.
6. It matches source and target columns by name instead of depending on position.
7. It has high cardinality and can create a very large number of small partitions.
8. Delta automatically creates and tracks the new partition through the transaction log.
9. Delta automatically tracks partitions and does not require manual partition registration.
10. It can create unnecessary folders, small files, metadata work, and slower operations.
11. Yes. Partition columns prune partitions, and non-partition filters can skip files inside them.
12. The data can be severely skewed, resulting in one huge partition and several small partitions.

</details>

---

# 34. Hands-On Tasks

## Task 1 — Add a fully static partition

Insert two orders into:

```text
order_year  = 2026
order_month = 4
```

Then verify the partition with `SHOW PARTITIONS`.

## Task 2 — Add dynamic rows

Create a source containing records from two different years and load them using `INSERT ... BY NAME`.

## Task 3 — Compare filter types

Run and inspect:

```text
Partition-column filter
Non-partition-column filter
Combined partition and non-partition filter
```

## Task 4 — Evaluate a partition key

For each proposed column, state whether it is suitable and explain why:

```text
order_year
order_month
customer_id
region
exact event timestamp
```

---

# 35. Final Summary

```text
Data skipping
    → Uses file-level statistics
    → Works with partitioned and unpartitioned Delta tables

Partition pruning
    → Uses partition-column filters
    → Skips complete partitions

Static partition insert
    → Partition value is fixed in the command

Dynamic partition load
    → Partition value comes from source records

Multi-level partitioning
    → Uses more than one top-level partition column

Good partition design
    → Frequent filters
    → Controlled cardinality
    → Large, balanced partitions

Poor partition design
    → High cardinality
    → Small files
    → Skew
    → Expensive future rewrite
```

The next session can build on this foundation with Z-ORDER and liquid clustering.

---

# 36. Optional Cleanup

Dropping the catalog removes the table metadata. The Python command then removes the dedicated external files.

```python
spark.sql(
    "DROP CATALOG IF EXISTS delta_partitioning_lab CASCADE"
)

dbutils.fs.rm(
    "abfss://data@demodb117.dfs.core.windows.net/"
    "databricks_training/delta_partitioning_session",
    True
)

print("Session catalog metadata and external files were removed.")
```
