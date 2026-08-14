# Delta Z-ORDER and File-Level Query Optimization

## Session Overview

Delta Lake can skip data files when file-level statistics prove that a file cannot contain the requested records. This works well only when the values used in filters are arranged effectively across the table files.

Z-ORDER improves that physical arrangement. It rewrites Delta data files so that records with related values are placed closer together. Delta data skipping can then eliminate more files during selective queries.

This session focuses only on Z-ORDER. Liquid clustering is covered separately so that the file-layout behavior of Z-ORDER can be observed clearly first.

---

# Learning Outcomes

By the end of this session, you should be able to:

- Explain the problem Z-ORDER is designed to solve.
- Connect Z-ORDER with Delta file-level statistics and data skipping.
- Differentiate normal `OPTIMIZE` from `OPTIMIZE ... ZORDER BY`.
- Create an external Delta table suitable for a Z-ORDER demonstration.
- Observe how one customer can be scattered across many files.
- Run Z-ORDER on a high-cardinality filter column.
- Compare active file ranges before and after Z-ORDER.
- Use query profile metrics to evaluate file pruning.
- Select suitable and unsuitable Z-ORDER columns.
- Explain how Z-ORDER behaves with partitioned tables.
- Explain what happens after new data is written to a Z-ordered table.
- Recognize the main limitations of Z-ORDER.

---

# Session Flow

```text
Recap file-level statistics and data skipping
        ↓
Understand the file-locality problem
        ↓
Create a fresh catalog and schema
        ↓
Generate a sales dataset with repeated customer IDs
        ↓
Create two identical external Delta tables
        ↓
Write the same data in twelve separate batches
        ↓
Inspect active files and customer-ID ranges
        ↓
Run a selective customer query
        ↓
Run normal OPTIMIZE on one table
        ↓
Understand bin-packing versus data co-location
        ↓
Run OPTIMIZE ZORDER BY customer_id on the second table
        ↓
Inspect active file ranges again
        ↓
Run the same selective query
        ↓
Compare files read and files pruned
        ↓
Discuss one-column and multi-column Z-ORDER
        ↓
Select good and bad Z-ORDER columns
        ↓
Discuss Z-ORDER with partitioned tables
        ↓
Discuss new writes and maintenance frequency
        ↓
Review scenarios and common interview questions
        ↓
Preview liquid clustering
```

---

# 1. Environment and Storage Details

The exercises use external Delta tables stored in Azure Data Lake Storage Gen2.

| Object | Value |
|---|---|
| ADLS container | `data` |
| Storage account | `demodb117` |
| Catalog | `delta_zorder_lab` |
| Schema | `sales_demo` |
| Root storage path | `abfss://data@demodb117.dfs.core.windows.net/databricks_training/delta_zorder_session` |

The session creates two tables with the same data:

```text
abfss://data@demodb117.dfs.core.windows.net/
└── databricks_training/
    └── delta_zorder_session/
        ├── sales_optimize_only/
        └── sales_zorder/
```

The first table is used to demonstrate normal `OPTIMIZE`.

The second table is used to demonstrate `OPTIMIZE ... ZORDER BY`.

Using two tables prevents one optimization from changing the physical layout used by the other demonstration.

## Required access

Before running the exercises, confirm that:

- Unity Catalog is enabled.
- The ADLS path is covered by a registered external location.
- You have permission to create a catalog and schema.
- You have `CREATE EXTERNAL TABLE` permission for the external location.
- Your Databricks compute can access Unity Catalog external tables.

---

# 2. Protected Cleanup for a Fresh Run

The following cell removes only the catalog and dedicated storage directory used in this session.

```python
# Remove catalog metadata created by an earlier run.
spark.sql("DROP CATALOG IF EXISTS delta_zorder_lab CASCADE")

# External table data remains after catalog objects are dropped.
# Remove only the dedicated directory used by this session.
dbutils.fs.rm(
    "abfss://data@demodb117.dfs.core.windows.net/"
    "databricks_training/delta_zorder_session",
    True
)

print("Earlier Z-ORDER session objects were removed.")
```

> Run this cell only when the path is dedicated to this exercise.

---

# 3. Create a Fresh Catalog and Schema

```sql
CREATE CATALOG IF NOT EXISTS delta_zorder_lab;

CREATE SCHEMA IF NOT EXISTS delta_zorder_lab.sales_demo;

USE CATALOG delta_zorder_lab;
USE SCHEMA sales_demo;
```

Confirm the namespace:

```sql
SELECT
    current_catalog() AS current_catalog,
    current_schema() AS current_schema;
```

Expected values:

```text
current_catalog = delta_zorder_lab
current_schema  = sales_demo
```

---

# 4. Recap: What Does Data Skipping Need?

Delta automatically stores file-level statistics for supported columns when data files are written.

Important statistics include:

```text
numRecords
minValues
maxValues
nullCount
```

Consider these three files:

```text
File A → customer_id 10000 to 14999
File B → customer_id 15000 to 19999
File C → customer_id 20000 to 24999
```

For this query:

```sql
SELECT *
FROM sales
WHERE customer_id = 10542;
```

Delta can reason that:

```text
File A → 10542 may exist → READ
File B → 10542 cannot exist → SKIP
File C → 10542 cannot exist → SKIP
```

Now consider a poorly organized layout:

```text
File A → customer_id 10000 to 59999
File B → customer_id 10000 to 59999
File C → customer_id 10000 to 59999
```

The same filter cannot eliminate any of these files using min/max statistics.

```text
Good locality
    → Narrower and less-overlapping file ranges
    → More files can be skipped

Poor locality
    → Wide and overlapping file ranges
    → More files must be considered
```

Z-ORDER is designed to improve this locality.

---

# 5. What Is Z-ORDER?

Z-ORDER is a Delta Lake data-layout technique that **co-locates related column values in the same set of data files**.

It is executed through `OPTIMIZE`:

```sql
OPTIMIZE table_name
ZORDER BY (column_name);
```

The important relationship is:

```text
Z-ORDER
    ↓
Rewrites physical Delta files
    ↓
Related values become more co-located
    ↓
File min/max ranges can become more selective
    ↓
Data skipping can eliminate more files
    ↓
Less data needs to be read for selective filters
```

Z-ORDER itself does not answer the query faster by creating an index. It improves the physical file organization so that Delta's existing data-skipping mechanism can work more effectively.

---

# 6. Z-ORDER Is Not the Same as ORDER BY

These operations solve different problems.

```text
ORDER BY
    → Controls the order in which query results are returned

Z-ORDER
    → Changes the physical organization of records across Delta files
```

For example:

```sql
SELECT *
FROM delta_zorder_lab.sales_demo.sales_zorder
ORDER BY customer_id;
```

`ORDER BY` guarantees result ordering for that query.

This command has a different purpose:

```sql
OPTIMIZE delta_zorder_lab.sales_demo.sales_zorder
ZORDER BY (customer_id);
```

It changes the table's physical file layout.

A normal query against the Z-ordered table is **not guaranteed** to return records in `customer_id` order unless the query itself contains `ORDER BY`.

---

# 7. Z-ORDER Is Not a Traditional Database Index

A traditional index usually creates a separate lookup structure.

Z-ORDER does not create an additional index object.

```text
Traditional database index
    → Separate lookup structure

Z-ORDER
    → Rewrites existing Delta data files
    → Improves value locality
    → Helps file-level data skipping
```

This distinction is important when discussing storage cost and maintenance.

---

# 8. Create the Source Dataset

The source contains **1,200,000 orders**.

The important distribution is:

```text
Total rows          → 1,200,000
Ingestion batches   → 12
Rows per batch      → 100,000
Distinct customers  → 50,000
Customer IDs        → 10000 to 59999
Dates                → Across 2025
Regions              → NORTH, SOUTH, EAST, WEST
Channels             → ONLINE, STORE, PARTNER
```

Each batch contains the complete customer-ID range more than once. This intentionally scatters the same customers across every ingestion batch.

For example, customer `10542` occurs in all twelve batches and has **24 records** in total.

Create the source view:

```sql
CREATE OR REPLACE TEMP VIEW zorder_source AS
SELECT
    CAST(id + 1000001 AS BIGINT) AS order_id,
    CAST(10000 + pmod(id, 50000) AS INT) AS customer_id,
    date_add(
        DATE '2025-01-01',
        CAST(pmod(id, 365) AS INT)
    ) AS order_date,
    CASE pmod(id, 4)
        WHEN 0 THEN 'NORTH'
        WHEN 1 THEN 'SOUTH'
        WHEN 2 THEN 'EAST'
        ELSE 'WEST'
    END AS region,
    CASE pmod(id, 6)
        WHEN 0 THEN 'ELECTRONICS'
        WHEN 1 THEN 'BOOKS'
        WHEN 2 THEN 'FASHION'
        WHEN 3 THEN 'HOME'
        WHEN 4 THEN 'SPORTS'
        ELSE 'GROCERY'
    END AS product_category,
    CASE pmod(id, 4)
        WHEN 0 THEN 'PLACED'
        WHEN 1 THEN 'SHIPPED'
        WHEN 2 THEN 'DELIVERED'
        ELSE 'CANCELLED'
    END AS order_status,
    CAST(
        100 + pmod(id * 37, 500000) / 100.0
        AS DECIMAL(12, 2)
    ) AS order_amount,
    CAST(id DIV 100000 AS INT) + 1 AS ingestion_batch,
    CASE pmod(id, 3)
        WHEN 0 THEN 'ONLINE'
        WHEN 1 THEN 'STORE'
        ELSE 'PARTNER'
    END AS sales_channel
FROM range(1200000);
```

Validate the generated data:

```sql
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT customer_id) AS distinct_customers,
    MIN(customer_id) AS minimum_customer_id,
    MAX(customer_id) AS maximum_customer_id,
    MIN(ingestion_batch) AS first_batch,
    MAX(ingestion_batch) AS last_batch
FROM zorder_source;
```

Expected result:

```text
total_rows          = 1,200,000
distinct_customers  = 50,000
minimum_customer_id = 10,000
maximum_customer_id = 59,999
first_batch         = 1
last_batch          = 12
```

Inspect the customer used throughout the exercise:

```sql
SELECT
    order_id,
    customer_id,
    order_date,
    region,
    product_category,
    order_status,
    order_amount,
    ingestion_batch,
    sales_channel
FROM zorder_source
WHERE customer_id = 10542
ORDER BY ingestion_batch, order_id;
```

Expected count:

```text
24 records
```

A few records for customer `10542` look like this:

| order_id | customer_id | order_date | region | product_category | order_status | order_amount | ingestion_batch | sales_channel |
|---:|---:|---|---|---|---|---:|---:|---|
| 1000543 | 10542 | 2025-06-27 | EAST | FASHION | DELIVERED | 300.54 | 1 | PARTNER |
| 1050543 | 10542 | 2025-06-22 | EAST | SPORTS | DELIVERED | 3800.54 | 1 | STORE |
| 1100543 | 10542 | 2025-06-17 | EAST | ELECTRONICS | DELIVERED | 2300.54 | 2 | ONLINE |
| 1150543 | 10542 | 2025-06-12 | EAST | FASHION | DELIVERED | 800.54 | 2 | PARTNER |
| 1200543 | 10542 | 2025-06-07 | EAST | SPORTS | DELIVERED | 4300.54 | 3 | STORE |
| 1250543 | 10542 | 2025-06-02 | EAST | ELECTRONICS | DELIVERED | 2800.54 | 3 | ONLINE |

The exact customer values are not important by themselves. What matters is that the same customer appears repeatedly across independently written batches.

---

# 9. Create Two Identical External Delta Tables

Create the table used for normal compaction:

```sql
CREATE TABLE delta_zorder_lab.sales_demo.sales_optimize_only
(
    order_id BIGINT,
    customer_id INT,
    order_date DATE,
    region STRING,
    product_category STRING,
    order_status STRING,
    order_amount DECIMAL(12, 2),
    ingestion_batch INT,
    sales_channel STRING
)
USING DELTA
LOCATION 'abfss://data@demodb117.dfs.core.windows.net/databricks_training/delta_zorder_session/sales_optimize_only';
```

Create the table used for Z-ORDER:

```sql
CREATE TABLE delta_zorder_lab.sales_demo.sales_zorder
(
    order_id BIGINT,
    customer_id INT,
    order_date DATE,
    region STRING,
    product_category STRING,
    order_status STRING,
    order_amount DECIMAL(12, 2),
    ingestion_batch INT,
    sales_channel STRING
)
USING DELTA
LOCATION 'abfss://data@demodb117.dfs.core.windows.net/databricks_training/delta_zorder_session/sales_zorder';
```

`customer_id` is the second table column, so it is included in the default file-statistics collection for this Unity Catalog external table.

---

# 10. Write the Same Data in Twelve Separate Batches

The purpose of separate writes is to make the physical layout intentionally less organized by `customer_id`.

Each batch contains customer IDs across the complete range.

```python
for batch_number in range(1, 13):
    # Read one ingestion batch and deliberately distribute it across tasks.
    batch_df = (
        spark.table("zorder_source")
        .where(f"ingestion_batch = {batch_number}")
        .repartition(4)
    )

    # Write the same batch to the normal OPTIMIZE comparison table.
    (
        batch_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("delta_zorder_lab.sales_demo.sales_optimize_only")
    )

    # Write the same batch to the table that will later be Z-ordered.
    (
        batch_df.write
        .format("delta")
        .mode("append")
        .saveAsTable("delta_zorder_lab.sales_demo.sales_zorder")
    )

    print(f"Finished ingestion batch {batch_number}")
```

> The exact number of output files can vary with Databricks Runtime, compute size, optimized writes, and file-size tuning. The important observation is that the same customer range is present across multiple independently written files.

Validate both tables:

```sql
SELECT
    'sales_optimize_only' AS table_name,
    COUNT(*) AS row_count
FROM delta_zorder_lab.sales_demo.sales_optimize_only

UNION ALL

SELECT
    'sales_zorder' AS table_name,
    COUNT(*) AS row_count
FROM delta_zorder_lab.sales_demo.sales_zorder;
```

Expected result:

```text
sales_optimize_only → 1,200,000
sales_zorder        → 1,200,000
```

---

# 11. Inspect the Current File Counts

```sql
DESCRIBE DETAIL delta_zorder_lab.sales_demo.sales_optimize_only;
```

```sql
DESCRIBE DETAIL delta_zorder_lab.sales_demo.sales_zorder;
```

Record these values for both tables:

```text
numFiles
sizeInBytes
```

The two tables should contain the same logical rows, although their exact physical file counts can vary slightly.

---

# 12. Observe Active File-Level Customer Ranges

The hidden `_metadata.file_path` field identifies the physical file from which each record is read.

The following query calculates the actual `customer_id` range present in every **active data file**.

```sql
SELECT
    _metadata.file_path AS file_path,
    COUNT(*) AS records_in_file,
    MIN(customer_id) AS minimum_customer_id,
    MAX(customer_id) AS maximum_customer_id
FROM delta_zorder_lab.sales_demo.sales_zorder
GROUP BY _metadata.file_path
ORDER BY minimum_customer_id, maximum_customer_id;
```

Before Z-ORDER, expect many files to have broad and overlapping ranges similar to:

```text
File A → 10002 to 59994
File B → 10001 to 59999
File C → 10000 to 59996
File D → 10004 to 59998
```

Your exact numbers will differ.

The important observation is:

```text
A filter for customer_id = 10542
can potentially match many files
because 10542 falls inside many file ranges.
```

---

# 13. Inspect the Stored Delta Statistics

You can also inspect the statistics recorded in the Delta transaction log.

Before either table is optimized, the transaction log contains only the files added by the batch writes, so the `add.stats` values provide a clear view of the original file ranges.

```python
from pyspark.sql import functions as F

stats_before = (
    spark.read
    .json(
        "abfss://data@demodb117.dfs.core.windows.net/"
        "databricks_training/delta_zorder_session/"
        "sales_zorder/_delta_log/*.json"
    )
    .where("add.path IS NOT NULL")
    .select(
        F.col("add.path").alias("data_file"),
        F.get_json_object(
            "add.stats",
            "$.numRecords"
        ).cast("long").alias("num_records"),
        F.get_json_object(
            "add.stats",
            "$.minValues.customer_id"
        ).cast("int").alias("min_customer_id"),
        F.get_json_object(
            "add.stats",
            "$.maxValues.customer_id"
        ).cast("int").alias("max_customer_id")
    )
    .where("num_records IS NOT NULL")
    .orderBy("min_customer_id", "max_customer_id")
)

display(stats_before)
```

Do not manually edit any file under `_delta_log`.

---

# 14. Run the Baseline Customer Query

Use a selective high-cardinality filter:

```sql
SELECT
    order_id,
    customer_id,
    order_date,
    region,
    product_category,
    order_amount,
    ingestion_batch
FROM delta_zorder_lab.sales_demo.sales_zorder
WHERE customer_id = 10542
ORDER BY order_date, order_id;
```

Expected result:

```text
24 rows
```

The result itself is not the performance measurement.

Open the query profile and record scan-related information such as:

```text
Files read
Files pruned / skipped
Bytes read
Percentage of data pruned
```

Do not judge the demonstration only by elapsed time. Runtime can be influenced by caching, cluster state, Photon, compute size, and concurrent workloads.

---

# 15. Normal OPTIMIZE: Compaction Without Z-ORDER

Before introducing Z-ORDER, separate **file compaction** from **value co-location**.

For this demonstration, use a smaller OPTIMIZE target file size so the sample table still contains several output files after compaction.

```sql
SET spark.databricks.delta.optimize.maxFileSize = 4194304;
```

`4194304` bytes is approximately 4 MB.

> This small target is only for making the file layout visible in a compact training dataset. It is not a recommended production file size.

Run normal `OPTIMIZE` on the comparison table:

```sql
OPTIMIZE delta_zorder_lab.sales_demo.sales_optimize_only;
```

Normal `OPTIMIZE` performs bin-packing:

```text
Many smaller files
        ↓
OPTIMIZE
        ↓
Fewer / better-sized files
```

Its primary goal here is file-size organization, not customer-value locality.

Inspect the table again:

```sql
DESCRIBE DETAIL delta_zorder_lab.sales_demo.sales_optimize_only;
```

Then inspect active customer ranges:

```sql
SELECT
    _metadata.file_path AS file_path,
    COUNT(*) AS records_in_file,
    MIN(customer_id) AS minimum_customer_id,
    MAX(customer_id) AS maximum_customer_id
FROM delta_zorder_lab.sales_demo.sales_optimize_only
GROUP BY _metadata.file_path
ORDER BY minimum_customer_id, maximum_customer_id;
```

You might now have fewer files, but the customer ranges can still overlap widely.

This gives us the first major distinction:

```text
OPTIMIZE
    → Improves file sizes through bin-packing

OPTIMIZE + ZORDER BY
    → Rewrites files and additionally co-locates selected values
```

---

# 16. Run Z-ORDER on `customer_id`

Now optimize the second table using the column frequently used in selective filters:

```sql
OPTIMIZE delta_zorder_lab.sales_demo.sales_zorder
ZORDER BY (customer_id);
```

`customer_id` is a strong demonstration column because:

- It has high cardinality.
- Queries can select a very small subset of customers.
- File statistics are available for it.
- The original writes deliberately scattered customer values across files.

The command rewrites the data files and attempts to co-locate nearby `customer_id` values.

---

# 17. Inspect the OPTIMIZE Result

The `OPTIMIZE` command itself returns metrics about files removed and added.

Also inspect the history:

```sql
DESCRIBE HISTORY delta_zorder_lab.sales_demo.sales_zorder LIMIT 3;
```

Look at the newest `OPTIMIZE` operation.

Useful operation metrics can include:

```text
numRemovedFiles
numAddedFiles
numRemovedBytes
numAddedBytes
minFileSize
p50FileSize
maxFileSize
```

The exact values depend on your generated files and compute environment.

---

# 18. Observe Active File Ranges After Z-ORDER

Run the same active-file analysis again:

```sql
SELECT
    _metadata.file_path AS file_path,
    COUNT(*) AS records_in_file,
    MIN(customer_id) AS minimum_customer_id,
    MAX(customer_id) AS maximum_customer_id
FROM delta_zorder_lab.sales_demo.sales_zorder
GROUP BY _metadata.file_path
ORDER BY minimum_customer_id, maximum_customer_id;
```

The ranges should now be noticeably more organized by `customer_id`.

Conceptually, the change should look more like this:

```text
Before Z-ORDER

File A → 10000 to 59990
File B → 10003 to 59999
File C → 10001 to 59995
File D → 10005 to 59998

After Z-ORDER

File A → 10000 to 15600
File B → 15601 to 21150
File C → 21151 to 26900
File D → 26901 to 32500
...
```

The exact boundaries are not guaranteed.

The important change is the reduction in overlapping customer ranges.

---


## 18.1 Inspect `_delta_log` Statistics After Z-ORDER

The active-file query above shows the actual values stored in the current Parquet files. We can also inspect the **file-level statistics written by the Z-ORDER `OPTIMIZE` transaction itself**.

This gives us a direct comparison with the statistics inspected earlier in **Section 13**.

First, identify the latest `OPTIMIZE` version for this table and read only that transaction-log JSON file:

```python
from pyspark.sql import functions as F

latest_zorder_version = (
    spark.sql(
        "DESCRIBE HISTORY delta_zorder_lab.sales_demo.sales_zorder"
    )
    .where(F.col("operation") == "OPTIMIZE")
    .orderBy(F.desc("version"))
    .select("version")
    .first()["version"]
)

print("Z-ORDER OPTIMIZE table version:", latest_zorder_version)

stats_after_zorder = (
    spark.read
    .json(
        f"abfss://data@demodb117.dfs.core.windows.net/"
        f"databricks_training/delta_zorder_session/"
        f"sales_zorder/_delta_log/{latest_zorder_version:020d}.json"
    )
    .where("add.path IS NOT NULL")
    .select(
        F.col("add.path").alias("data_file"),
        F.get_json_object(
            "add.stats",
            "$.numRecords"
        ).cast("long").alias("num_records"),
        F.get_json_object(
            "add.stats",
            "$.minValues.customer_id"
        ).cast("int").alias("min_customer_id"),
        F.get_json_object(
            "add.stats",
            "$.maxValues.customer_id"
        ).cast("int").alias("max_customer_id")
    )
    .where("num_records IS NOT NULL")
    .orderBy("min_customer_id", "max_customer_id")
)

display(stats_after_zorder)
```

Now compare this output with the `stats_before` output from Section 13.

The exact ranges depend on the files created in your workspace, but the pattern should become clearer:

```text
Before Z-ORDER
File A → customer_id 10000 to 59990
File B → customer_id 10003 to 59999
File C → customer_id 10001 to 59995

After Z-ORDER
File X → customer_id 10000 to 15600
File Y → customer_id 15601 to 21150
File Z → customer_id 21151 to 26900
```

Focus on **overlap between file ranges**, not the exact boundary values.

```text
Before Z-ORDER
    → Wide, overlapping min/max ranges
    → Many files remain possible candidates

After Z-ORDER
    → More localized min/max ranges
    → More files can be eliminated by data skipping
```

This inspection connects the entire idea:

```text
Z-ORDER rewrites the Parquet files
        ↓
Records with related customer_id values become more co-located
        ↓
New files receive different min/max statistics
        ↓
Those statistics are stored in the Delta transaction log
        ↓
Databricks can skip more irrelevant files during selective queries
```

> Reading `_delta_log` directly is useful for understanding Delta internals. Application logic should query the Delta table rather than depending directly on the physical transaction-log JSON layout.

# 19. Why Does This Improve Data Skipping?

Consider the query:

```sql
SELECT *
FROM delta_zorder_lab.sales_demo.sales_zorder
WHERE customer_id = 10542;
```

Before Z-ORDER:

```text
10542 is inside the min/max range of many files
        ↓
Many files remain candidates
```

After Z-ORDER:

```text
10542 is inside the range of only a smaller set of files
        ↓
More files can be eliminated using statistics
```

Z-ORDER and data skipping therefore work together:

```text
Z-ORDER improves locality
        ↓
Locality improves file statistics
        ↓
File statistics enable data skipping
```

---

# 20. Run the Same Query After Z-ORDER

```sql
SELECT
    order_id,
    customer_id,
    order_date,
    region,
    product_category,
    order_amount,
    ingestion_batch
FROM delta_zorder_lab.sales_demo.sales_zorder
WHERE customer_id = 10542
ORDER BY order_date, order_id;
```

The result must still contain:

```text
24 rows
```

Open the query profile and compare the scan metrics with the earlier execution.

Focus on:

```text
Files read
Files pruned
Bytes read
Percentage of data pruned
```

The objective is:

```text
Same logical result
+
Less unnecessary file scanning
```

---

# 21. Compare Normal OPTIMIZE with Z-ORDER

Run the same customer filter against the table that received only normal `OPTIMIZE`:

```sql
SELECT
    order_id,
    customer_id,
    order_date,
    region,
    product_category,
    order_amount,
    ingestion_batch
FROM delta_zorder_lab.sales_demo.sales_optimize_only
WHERE customer_id = 10542
ORDER BY order_date, order_id;
```

Both optimized tables contain the same 1,200,000 records and return the same 24 records for customer `10542`.

The difference is the physical layout strategy.

| Normal `OPTIMIZE` | `OPTIMIZE ZORDER BY` |
|---|---|
| Primarily bin-packs files | Bin-packs and co-locates selected values |
| Improves file-size layout | Improves file-size and selected-column locality |
| Does not specifically organize by `customer_id` | Organizes around `customer_id` locality |
| Can reduce file-management overhead | Can additionally improve selective data skipping |

---

# 22. Choosing a Good Z-ORDER Column

A useful Z-ORDER column usually has these characteristics:

```text
Frequently used in WHERE predicates
        +
Selective filters
        +
High or meaningful cardinality
        +
File-level statistics are collected
```

Good candidates can include:

```text
customer_id
device_id
account_id
transaction_id
event_timestamp
order_id
```

The exact choice depends on real query patterns.

## Why high-cardinality columns can be useful

Partitioning a table by millions of customer IDs would create an excessive number of partitions.

Z-ORDER does not create one physical partition per customer.

This makes columns such as `customer_id` much more practical for Z-ORDER than for traditional partitioning.

---

# 23. Less Useful Z-ORDER Columns

Avoid choosing a column only because it exists in the table.

A column can be a weak Z-ORDER candidate when:

- Queries rarely filter by it.
- Most queries still return a large percentage of the table.
- It has no file-level statistics collected.
- It contains very long string values that are not useful for filtering.
- Many other Z-ORDER columns have already been selected.

For this dataset, consider `order_status`:

```text
PLACED
SHIPPED
DELIVERED
CANCELLED
```

It has only four values and a filter such as:

```sql
WHERE order_status = 'DELIVERED'
```

can still select roughly one quarter of the table.

This does not mean a low-cardinality column can never benefit from locality. It means the expected reduction in scanned data should justify the rewrite cost.

---

# 24. Z-ORDER Requires Statistics on the Selected Column

Delta data skipping depends on file-level statistics.

Databricks therefore recommends Z-ordering only columns for which statistics are collected.

For Unity Catalog external Delta tables, statistics are collected on the first 32 table columns by default unless the configuration is changed.

Our table has only nine columns and `customer_id` is the second column, so no additional statistics configuration is required for this exercise.

If a useful filter column were outside the configured statistics set, you could explicitly configure statistics columns:

```sql
ALTER TABLE delta_zorder_lab.sales_demo.sales_zorder
SET TBLPROPERTIES
(
    'delta.dataSkippingStatsColumns' =
    'customer_id,order_date,region'
);
```

Changing this property affects future statistics collection.

To recompute Delta statistics for existing files on supported runtimes:

```sql
ANALYZE TABLE delta_zorder_lab.sales_demo.sales_zorder
COMPUTE DELTA STATISTICS;
```

Do not run this after every normal write. Newly written Delta files automatically receive statistics for the currently configured statistics columns.

---

# 25. Z-ORDER with Multiple Columns

Z-ORDER supports more than one column:

```sql
OPTIMIZE delta_zorder_lab.sales_demo.sales_zorder
ZORDER BY (customer_id, order_date);
```

This can help when important queries frequently filter on both dimensions, for example:

```sql
SELECT *
FROM delta_zorder_lab.sales_demo.sales_zorder
WHERE customer_id = 10542
  AND order_date BETWEEN DATE '2025-04-01'
                     AND DATE '2025-06-30';
```

However, adding more Z-ORDER columns is not automatically better.

```text
One selected column
    → Locality effort concentrated on one dimension

Multiple selected columns
    → Locality benefit is distributed across dimensions
```

Databricks notes that Z-ORDER effectiveness decreases as additional columns are added.

Choose a small number of columns that match real filtering patterns rather than Z-ordering every frequently mentioned column.

> Keep the multi-column command as an optional extension after the one-column demonstration. Running it changes the physical layout created by the earlier `customer_id`-only exercise.

---

# 26. Z-ORDER and Partitioned Tables

Z-ORDER can also be used with an existing partitioned Delta table.

Suppose a table is partitioned by `order_year` and queries frequently filter by `customer_id`.

A valid strategy can be:

```text
Partition by order_year
        ↓
Z-ORDER by customer_id inside each partition
```

Example syntax:

```sql
OPTIMIZE some_catalog.some_schema.partitioned_orders
WHERE order_year = 2026
ZORDER BY (customer_id);
```

For a partitioned table:

- `OPTIMIZE` cannot combine files across partition boundaries.
- Z-ORDER happens within each selected partition.
- The `WHERE` clause for this type of `OPTIMIZE` targets partition columns.
- A partition column itself cannot also be used as a Z-ORDER column.

For example, if the table is already partitioned by `order_year`, do not use:

```sql
-- Not a valid layout choice when order_year is the partition column.
OPTIMIZE some_catalog.some_schema.partitioned_orders
ZORDER BY (order_year);
```

The existing partition boundary already organizes the data by that column.

---

# 27. What Happens When New Data Arrives After Z-ORDER?

Z-ORDER is a file-rewrite operation. It is not automatically applied to every future append.

Consider this sequence:

```text
Monday
    → Table Z-ordered by customer_id

Tuesday
    → New batch appended

Wednesday
    → Another batch appended
```

The new files have valid Delta statistics, but they are not guaranteed to follow the earlier Z-ORDER layout.

If the table continues to use a Z-ORDER strategy, maintenance can run again:

```sql
OPTIMIZE delta_zorder_lab.sales_demo.sales_zorder
ZORDER BY (customer_id);
```

Z-ORDER operates incrementally. If no new data has been added to data that was just Z-ordered, immediately running the same Z-ORDER again generally has no additional effect on that data.

The correct maintenance frequency depends on:

```text
How frequently data arrives
How quickly query performance degrades
Table size
OPTIMIZE compute cost
Query importance
```

Do not run Z-ORDER after every small write by default.

---

# 28. Does Predictive Optimization Automatically Run Z-ORDER?

No.

Predictive optimization can automatically run maintenance such as normal `OPTIMIZE`, `VACUUM`, and `ANALYZE` for eligible Unity Catalog managed tables.

It does **not** automatically execute your Z-ORDER strategy.

If an existing table relies on Z-ORDER, that Z-ORDER maintenance must still be planned explicitly.

This is another reason current Databricks guidance prefers liquid clustering for new tables.

---

# 29. What Happens to the Old Files After Z-ORDER?

Z-ORDER runs through `OPTIMIZE`, which rewrites data files.

Conceptually:

```text
Old active files
        ↓
OPTIMIZE ZORDER BY
        ↓
New reorganized files are added
        ↓
Old files are marked as removed in the Delta log
```

The old physical files do not have to disappear immediately.

They remain available until they become eligible for `VACUUM` according to the table's deleted-file retention policy.

This is the same Delta behavior discussed during the earlier `OPTIMIZE` and `VACUUM` exercises.

---

# 30. Z-ORDER Does Not Change the Logical Data

Run a count before and after Z-ORDER:

```sql
SELECT COUNT(*) AS row_count
FROM delta_zorder_lab.sales_demo.sales_zorder;
```

Expected result:

```text
1,200,000
```

Z-ORDER changes physical file organization, not the logical table contents.

```text
Before Z-ORDER → Same rows
After Z-ORDER  → Same rows

Difference     → Physical file layout
```

---

# 31. Practical Decision Scenarios

## Scenario 1: Customer transaction table

```text
Table size:
Large

Distinct customers:
Millions

Frequent query:
WHERE customer_id = ?
```

Traditional partitioning by `customer_id` would create far too many partitions.

For an existing non-liquid-clustered Delta table, `customer_id` can be a strong Z-ORDER candidate because it has high cardinality and is frequently used in selective filters.

---

## Scenario 2: Order-status dashboard

```text
Possible statuses:
PLACED
SHIPPED
DELIVERED
CANCELLED
```

A query filtering one status can still read a large fraction of the table.

Do not automatically choose `order_status` for Z-ORDER simply because it appears in a filter.

First ask how selective the predicate really is.

---

## Scenario 3: Existing table partitioned by year

```text
Partition column:
order_year

Frequent search column:
customer_id
```

Possible approach:

```sql
OPTIMIZE some_catalog.some_schema.orders
WHERE order_year = 2026
ZORDER BY (customer_id);
```

Partition pruning reduces the year first, and Z-ORDER can improve file skipping within that partition.

---

## Scenario 4: Partitioned by customer_id

```text
Millions of customers
Partition column = customer_id
```

This is usually a poor partition design because it can produce an excessive number of small partitions.

Z-ORDER does not require one physical partition per customer and is therefore better suited to this type of high-cardinality access pattern for an existing non-liquid-clustered table.

---

## Scenario 5: New table being designed today

If you are designing a new Delta table on current Databricks runtimes, Databricks recommends evaluating **liquid clustering** instead of introducing a new Z-ORDER strategy.

Z-ORDER remains important because many production Delta tables already use it and it clearly demonstrates how value locality improves data skipping.

---

## Scenario 6: Queries filter on five different columns

Do not immediately run:

```sql
OPTIMIZE some_catalog.some_schema.large_table
ZORDER BY (col1, col2, col3, col4, col5);
```

The locality benefit decreases as more dimensions are added.

Identify the most important selective query patterns and choose a small number of useful columns.

---

# 32. Common Interview Questions

## Q1. What problem does Z-ORDER solve?

Z-ORDER improves the physical locality of selected column values across Delta files so that file-level data skipping can eliminate more irrelevant files.

---

## Q2. Does Z-ORDER create an index?

No. It rewrites Delta data files. It does not create a separate index structure.

---

## Q3. Is Z-ORDER the same as sorting data?

No. It changes physical value locality across data files. Query result ordering still requires `ORDER BY`.

---

## Q4. What is the difference between `OPTIMIZE` and `OPTIMIZE ZORDER BY`?

```text
OPTIMIZE
    → Bin-packs files primarily for better file sizing

OPTIMIZE ZORDER BY
    → Also co-locates values for selected columns
```

---

## Q5. Which columns are good candidates for Z-ORDER?

Columns frequently used in selective query predicates, especially meaningful high-cardinality columns with file-level statistics available.

---

## Q6. Should every filter column be included in Z-ORDER?

No. Effectiveness decreases as more columns are added. Choose columns based on important query patterns.

---

## Q7. Can we Z-ORDER a partition column?

No. A column used for partitioning cannot also be used as a Z-ORDER column.

---

## Q8. Can a partitioned table use Z-ORDER?

Yes. Z-ORDER operates within partition boundaries and cannot combine data across separate partitions.

---

## Q9. Do we need statistics for a Z-ORDER column?

Yes. Z-ORDER should be used on columns for which file-level statistics are collected because data skipping depends on those statistics.

---

## Q10. Does Z-ORDER need to run after every append?

No. New writes are not automatically Z-ordered, but maintenance frequency should be based on data arrival rate, query needs, table size, and optimization cost.

---

## Q11. Does predictive optimization automatically maintain Z-ORDER?

No. Predictive optimization does not automatically execute `ZORDER BY`.

---

## Q12. Can Z-ORDER and liquid clustering be used on the same table?

No. Liquid clustering and Z-ORDER are incompatible layout strategies for the same table.

---

# 33. Observation Worksheet

Record the values observed in your Databricks environment.

| Observation | Before optimization | Normal OPTIMIZE | Z-ORDER |
|---|---:|---:|---:|
| Total rows | | | |
| Number of active files | | | |
| Smallest customer ID | | | |
| Largest customer ID | | | |
| Typical overlap between file ranges | | | |
| Files read for customer 10542 | | | |
| Files pruned for customer 10542 | | | |
| Bytes read | | | |

The exact numbers are environment dependent. The main comparison is whether the Z-ordered layout produces better `customer_id` locality and better pruning for the selective filter.

---

# 34. Troubleshooting

## The OPTIMIZE result creates only one file

If the entire demonstration table becomes one file, data skipping cannot visibly demonstrate file elimination because there are no other active files to skip.

This session temporarily sets:

```sql
SET spark.databricks.delta.optimize.maxFileSize = 4194304;
```

to encourage multiple output files for the demonstration dataset.

Exact file counts are still not guaranteed.

---

## The file count before Z-ORDER is different from the expected count

This is normal.

Databricks can apply optimized writes and other runtime-specific file-size behavior. Focus on file ranges and query-pruning behavior rather than expecting one exact number of files.

---

## Query time does not improve noticeably

The sample table is much smaller than a production table. Caching, Photon, cluster state, and compute size can dominate elapsed time.

Compare:

```text
Files read
Files pruned
Bytes read
File min/max ranges
```

instead of relying only on execution seconds.

---

## Z-ORDER shows little improvement

Check:

- Are there enough active files for skipping to matter?
- Is `customer_id` included in collected statistics?
- Is the query selective?
- Were customer values already well organized before Z-ORDER?
- Did the table collapse into too few files?

---

# 35. Restore the Demonstration File-Size Setting

The session changed the OPTIMIZE target file size only to keep several files visible in this small dataset.

Restore the Databricks Runtime default used by `OPTIMIZE`:

```sql
SET spark.databricks.delta.optimize.maxFileSize = 1073741824;
```

`1073741824` bytes is 1 GB.

---

# 36. Cleanup

Drop the catalog metadata:

```sql
DROP CATALOG IF EXISTS delta_zorder_lab CASCADE;
```

Because these are external Delta tables, dropping the catalog does not remove the physical ADLS data.

Remove only the session directory if you no longer need it:

```python
dbutils.fs.rm(
    "abfss://data@demodb117.dfs.core.windows.net/"
    "databricks_training/delta_zorder_session",
    True
)

print("Z-ORDER session files were removed.")
```

---

# 37. Final Summary

```text
Data skipping
    → Uses file-level statistics

Poor file locality
    → File min/max ranges overlap
    → More files remain candidates

Normal OPTIMIZE
    → Improves file sizing through bin-packing

Z-ORDER
    → Co-locates selected values during OPTIMIZE
    → Can create more selective file ranges
    → Helps data skipping eliminate more files
```

Remember the main decision rule:

```text
Frequently filtered
+
Selective
+
Statistics available
+
Existing non-liquid-clustered Delta table
        ↓
Potential Z-ORDER candidate
```

For new Delta table designs, current Databricks guidance is to prefer liquid clustering. That topic is covered in the next session.

