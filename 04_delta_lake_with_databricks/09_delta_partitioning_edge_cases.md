# Delta Partitioning: Edge Cases and Interview Scenarios

## Session Overview

This guide focuses on practical partitioning situations that commonly appear in real projects and interviews.

Every scenario uses its own table and its own ADLS directory. You can run any scenario independently without depending on tables from earlier exercises.

The exercises use external Delta tables stored in Azure Data Lake Storage Gen2.

---

# Learning Outcomes

By the end of this guide, you should be able to:

- Explain how to change the partitioning strategy of an existing Delta table.
- Handle `NULL` values in a partition column.
- Register an existing partitioned Delta location as a table.
- Understand the difference between an existing Delta location and an existing partitioned Parquet location.
- Convert existing partitioned Parquet data to Delta Lake.
- Explain why manually copied Parquet files are not automatically part of a Delta table.
- Explain what happens when an `UPDATE` changes a partition-column value.
- Recognize over-partitioning caused by high-cardinality columns.
- Answer common partitioning interview scenarios with clear reasoning.

---

# Environment

| Object | Value |
|---|---|
| ADLS container | `data` |
| Storage account | `demodb117` |
| Catalog | `delta_partitioning_scenarios` |
| Schema | `cases` |
| Root path | `abfss://data@demodb117.dfs.core.windows.net/databricks_training/delta_partitioning_scenarios` |

The root path is divided into separate directories for each scenario.

```text
abfss://data@demodb117.dfs.core.windows.net/
└── databricks_training/
    └── delta_partitioning_scenarios/
        ├── change_partition_source/
        ├── change_partition_target/
        ├── null_partition/
        ├── existing_delta_location/
        ├── existing_partitioned_parquet/
        ├── manual_file_table/
        ├── manual_file_staging/
        ├── update_partition_column/
        └── high_cardinality_partition/
```

---

# 1. Fresh Setup

Run this once before starting the scenarios.

```python
# Remove catalog metadata from an earlier run.
spark.sql("DROP CATALOG IF EXISTS delta_partitioning_scenarios CASCADE")

# External-table data remains after the catalog is dropped,
# so remove only the directory dedicated to these exercises.
dbutils.fs.rm(
    "abfss://data@demodb117.dfs.core.windows.net/"
    "databricks_training/delta_partitioning_scenarios",
    True
)

print("Earlier scenario metadata and files were removed.")
```

> Run the cleanup only when this directory is dedicated to these exercises.

Create the catalog and schema:

```sql
CREATE CATALOG IF NOT EXISTS delta_partitioning_scenarios;

CREATE SCHEMA IF NOT EXISTS delta_partitioning_scenarios.cases;

USE CATALOG delta_partitioning_scenarios;
USE SCHEMA cases;
```

Confirm the namespace:

```sql
SELECT
    current_catalog() AS current_catalog,
    current_schema() AS current_schema;
```

---

# Scenario 1: What If We Need to Change the Partitioning Column?

## Situation

A table was originally partitioned by `order_year`:

```text
PARTITIONED BY (order_year)
```

Later, query patterns change and most queries filter by `region`.

The requirement becomes:

```text
Old layout → partition by order_year
New layout → partition by region
```

## Important Rule

For a traditionally partitioned Delta table, changing from one partitioning specification to another is not treated as a small metadata-only change.

The safe approach is to rewrite the data into a new table or new location using the new partitioning strategy. Newer Databricks runtimes also provide a separate path for converting a partitioned table to liquid clustering, but that is a different operation and will be covered with liquid clustering.

```text
Existing partitioning
        ↓
Create a new table with the desired partitioning
        ↓
Rewrite or migrate the records
        ↓
Validate the new table
        ↓
Move consumers only after validation
```

## Step 1: Create the original table

```sql
CREATE TABLE delta_partitioning_scenarios.cases.orders_partitioned_by_year
(
    order_id BIGINT,
    order_date DATE,
    order_year INT,
    region STRING,
    order_amount DECIMAL(12,2)
)
USING DELTA
PARTITIONED BY (order_year)
LOCATION 'abfss://data@demodb117.dfs.core.windows.net/databricks_training/delta_partitioning_scenarios/change_partition_source';
```

Insert visible data across two years and multiple regions:

```sql
INSERT INTO delta_partitioning_scenarios.cases.orders_partitioned_by_year
VALUES
    (1001, DATE '2025-01-10', 2025, 'NORTH', 1200.00),
    (1002, DATE '2025-02-15', 2025, 'SOUTH', 1800.00),
    (1003, DATE '2025-03-18', 2025, 'WEST',  2500.00),
    (2001, DATE '2026-01-11', 2026, 'NORTH', 3100.00),
    (2002, DATE '2026-02-17', 2026, 'EAST',  4200.00),
    (2003, DATE '2026-03-20', 2026, 'WEST',  5100.00);
```

Inspect the current partitions:

```sql
SHOW PARTITIONS delta_partitioning_scenarios.cases.orders_partitioned_by_year;
```

Expected logical partitions:

```text
order_year=2025
order_year=2026
```

## Step 2: Create a new table with the desired partitioning

```sql
CREATE TABLE delta_partitioning_scenarios.cases.orders_partitioned_by_region
(
    order_id BIGINT,
    order_date DATE,
    order_year INT,
    region STRING,
    order_amount DECIMAL(12,2)
)
USING DELTA
PARTITIONED BY (region)
LOCATION 'abfss://data@demodb117.dfs.core.windows.net/databricks_training/delta_partitioning_scenarios/change_partition_target';
```

Migrate the records:

```sql
INSERT INTO delta_partitioning_scenarios.cases.orders_partitioned_by_region
SELECT
    order_id,
    order_date,
    order_year,
    region,
    order_amount
FROM delta_partitioning_scenarios.cases.orders_partitioned_by_year;
```

Inspect the new partitions:

```sql
SHOW PARTITIONS delta_partitioning_scenarios.cases.orders_partitioned_by_region;
```

Expected logical partitions:

```text
region=EAST
region=NORTH
region=SOUTH
region=WEST
```

Validate both tables:

```sql
SELECT COUNT(*) AS source_count
FROM delta_partitioning_scenarios.cases.orders_partitioned_by_year;
```

```sql
SELECT COUNT(*) AS target_count
FROM delta_partitioning_scenarios.cases.orders_partitioned_by_region;
```

Both counts should be `6`.

## Interview Takeaway

**Question:** Can we simply alter a Delta table from `PARTITIONED BY (order_year)` to `PARTITIONED BY (region)`?

**Answer:** For traditional Delta partitioning, changing the partitioning strategy normally requires rewriting the table into the desired layout. Treat it as a data-layout migration rather than a small metadata change.

---

# Scenario 2: What Happens When the Partition Column Contains NULL?

## Situation

A table is partitioned by `region`, but some records arrive without a region value.

Example:

```text
order_id = 3003
region   = NULL
```

A nullable partition column can contain `NULL`.

## Create an independent table

```sql
CREATE TABLE delta_partitioning_scenarios.cases.orders_with_null_partition
(
    order_id BIGINT,
    customer_name STRING,
    region STRING,
    order_amount DECIMAL(12,2)
)
USING DELTA
PARTITIONED BY (region)
LOCATION 'abfss://data@demodb117.dfs.core.windows.net/databricks_training/delta_partitioning_scenarios/null_partition';
```

Insert records including two rows with a null region:

```sql
INSERT INTO delta_partitioning_scenarios.cases.orders_with_null_partition
VALUES
    (3001, 'Aarav', 'NORTH', 1200.00),
    (3002, 'Meera', 'SOUTH', 1850.00),
    (3003, 'Kabir', NULL,    2200.00),
    (3004, 'Riya',  'WEST',  3100.00),
    (3005, 'Nisha', NULL,     975.00);
```

Inspect the logical partitions:

```sql
SHOW PARTITIONS delta_partitioning_scenarios.cases.orders_with_null_partition;
```

Observe how the current runtime represents the null partition.

Query only the null partition logically:

```sql
SELECT *
FROM delta_partitioning_scenarios.cases.orders_with_null_partition
WHERE region IS NULL
ORDER BY order_id;
```

Expected records:

| order_id | customer_name | region | order_amount |
|---:|---|---|---:|
| 3003 | Kabir | NULL | 2200.00 |
| 3005 | Nisha | NULL | 975.00 |

Inspect the physical directories:

```python
display(
    dbutils.fs.ls(
        "abfss://data@demodb117.dfs.core.windows.net/"
        "databricks_training/delta_partitioning_scenarios/null_partition"
    )
)
```

## Important Point

Do not build application logic around the exact physical directory name Databricks uses to represent a null partition.

Use table queries:

```sql
WHERE region IS NULL
```

instead of attempting to directly read a special partition directory.

## Interview Takeaway

**Question:** Are null values allowed in a Delta partition column?

**Answer:** Yes, if the column is nullable. Delta tracks those records as part of the table and they can be queried using `IS NULL`. A large number of null values can create a heavily skewed partition, so the data distribution should still be evaluated.

---

# Scenario 3: Existing Storage Location Already Contains a Partitioned Delta Table

## Situation

An ADLS directory already contains:

- Delta Parquet data files
- `_delta_log`
- Existing partition metadata

The original catalog table was removed, but the storage data still exists.

Can we create another external table using the same Delta location?

Yes. If the location already contains a valid Delta table, the Delta log is the source of truth for its schema and partitioning.

## Step 1: Create and populate a temporary catalog entry

```sql
CREATE TABLE delta_partitioning_scenarios.cases.existing_delta_original
(
    order_id BIGINT,
    region STRING,
    order_amount DECIMAL(12,2)
)
USING DELTA
PARTITIONED BY (region)
LOCATION 'abfss://data@demodb117.dfs.core.windows.net/databricks_training/delta_partitioning_scenarios/existing_delta_location';
```

```sql
INSERT INTO delta_partitioning_scenarios.cases.existing_delta_original
VALUES
    (4001, 'NORTH', 1000.00),
    (4002, 'SOUTH', 1500.00),
    (4003, 'WEST',  2500.00),
    (4004, 'NORTH', 3200.00);
```

Confirm the partitions:

```sql
SHOW PARTITIONS delta_partitioning_scenarios.cases.existing_delta_original;
```

## Step 2: Drop only the table metadata

```sql
DROP TABLE delta_partitioning_scenarios.cases.existing_delta_original;
```

Because this is an external table, the ADLS data and `_delta_log` remain.

Confirm the files still exist:

```python
display(
    dbutils.fs.ls(
        "abfss://data@demodb117.dfs.core.windows.net/"
        "databricks_training/delta_partitioning_scenarios/existing_delta_location"
    )
)
```

## Step 3: Demonstrate a mismatched registration

The existing Delta table is partitioned by `region`.

The following command intentionally declares the wrong partition column.

### Expected failure

```sql
CREATE TABLE delta_partitioning_scenarios.cases.existing_delta_wrong_definition
(
    order_id BIGINT,
    region STRING,
    order_amount DECIMAL(12,2)
)
USING DELTA
PARTITIONED BY (order_id)
LOCATION 'abfss://data@demodb117.dfs.core.windows.net/databricks_training/delta_partitioning_scenarios/existing_delta_location';
```

The declared table definition does not match the metadata already stored in the Delta log.

## Step 4: Register the location correctly

Let Delta read the existing metadata from the location:

```sql
CREATE TABLE delta_partitioning_scenarios.cases.existing_delta_registered
USING DELTA
LOCATION 'abfss://data@demodb117.dfs.core.windows.net/databricks_training/delta_partitioning_scenarios/existing_delta_location';
```

Validate the data:

```sql
SELECT *
FROM delta_partitioning_scenarios.cases.existing_delta_registered
ORDER BY order_id;
```

Inspect the partitions:

```sql
SHOW PARTITIONS delta_partitioning_scenarios.cases.existing_delta_registered;
```

## Interview Takeaway

**Question:** If a storage location already contains a Delta table, do we need to redefine its partitions when registering it again?

**Answer:** No. The existing Delta log already records the schema, table configuration, and partition columns. The cleanest registration is `CREATE TABLE ... USING DELTA LOCATION ...`. If you explicitly provide schema or partitioning, it must match the existing Delta metadata.

---

# Scenario 4: Existing Storage Contains Partitioned Parquet, Not Delta

## Situation

An ADLS location contains directories such as:

```text
region=NORTH/
region=SOUTH/
region=WEST/
```

but there is no `_delta_log` directory.

This is partitioned Parquet data, not a Delta table.

Creating a Delta table over that location does not automatically convert the existing Parquet files into a Delta table.

## Step 1: Create partitioned Parquet data

```python
(
    spark.sql(
        """
        SELECT *
        FROM VALUES
            (CAST(5001 AS BIGINT), 'NORTH', CAST(1200.00 AS DECIMAL(12,2))),
            (CAST(5002 AS BIGINT), 'SOUTH', CAST(1800.00 AS DECIMAL(12,2))),
            (CAST(5003 AS BIGINT), 'WEST',  CAST(2500.00 AS DECIMAL(12,2))),
            (CAST(5004 AS BIGINT), 'NORTH', CAST(3200.00 AS DECIMAL(12,2)))
        AS source(order_id, region, order_amount)
        """
    )
    .write
    .mode("overwrite")
    .partitionBy("region")
    .parquet(
        "abfss://data@demodb117.dfs.core.windows.net/"
        "databricks_training/delta_partitioning_scenarios/existing_partitioned_parquet"
    )
)
```

Inspect the location:

```python
display(
    dbutils.fs.ls(
        "abfss://data@demodb117.dfs.core.windows.net/"
        "databricks_training/delta_partitioning_scenarios/existing_partitioned_parquet"
    )
)
```

You should see partition directories but no Delta `_delta_log` yet.

## Step 2: Attempt to treat the location as Delta

### Expected failure

```sql
CREATE TABLE delta_partitioning_scenarios.cases.partitioned_parquet_as_delta_wrong
USING DELTA
LOCATION 'abfss://data@demodb117.dfs.core.windows.net/databricks_training/delta_partitioning_scenarios/existing_partitioned_parquet';
```

The location contains Parquet files but does not yet contain a Delta transaction log.

## Step 3: Convert the partitioned Parquet directory to Delta

Because the data is partitioned by `region`, include the partition specification during path-based conversion:

```sql
CONVERT TO DELTA
parquet.`abfss://data@demodb117.dfs.core.windows.net/databricks_training/delta_partitioning_scenarios/existing_partitioned_parquet`
PARTITIONED BY (region STRING);
```

Inspect the location again:

```python
display(
    dbutils.fs.ls(
        "abfss://data@demodb117.dfs.core.windows.net/"
        "databricks_training/delta_partitioning_scenarios/existing_partitioned_parquet"
    )
)
```

A `_delta_log` directory should now exist.

## Step 4: Register the converted location

```sql
CREATE TABLE delta_partitioning_scenarios.cases.converted_partitioned_delta
USING DELTA
LOCATION 'abfss://data@demodb117.dfs.core.windows.net/databricks_training/delta_partitioning_scenarios/existing_partitioned_parquet';
```

Validate the rows:

```sql
SELECT *
FROM delta_partitioning_scenarios.cases.converted_partitioned_delta
ORDER BY order_id;
```

Inspect the partitions:

```sql
SHOW PARTITIONS delta_partitioning_scenarios.cases.converted_partitioned_delta;
```

## Interview Takeaway

**Question:** A directory contains `region=NORTH`, `region=SOUTH`, and other Parquet partition folders. Can we directly register it as a Delta table?

**Answer:** Not unless the directory already has a valid Delta transaction log. For existing partitioned Parquet data, convert it to Delta first, or register it as Parquet. For path-based `CONVERT TO DELTA`, provide the partition specification for partitioned data.

---

# Scenario 5: A Parquet File Is Manually Added to the Root of a Partitioned Delta Table

## Situation

A Delta table is partitioned by `region`.

Someone manually copies a Parquet file directly into the table root instead of writing through Delta Lake.

Example physical layout:

```text
manual_file_table/
├── _delta_log/
├── region=NORTH/
├── region=SOUTH/
└── rogue_manual_file.parquet   ← copied manually
```

Will the new file automatically become part of the Delta table?

**No.**

Delta does not discover arbitrary Parquet files by scanning the storage directory during every query. The transaction log determines which files belong to the table.

## Step 1: Create a fresh partitioned Delta table

```sql
CREATE TABLE delta_partitioning_scenarios.cases.orders_manual_file_demo
(
    order_id BIGINT,
    customer_name STRING,
    region STRING,
    order_amount DECIMAL(12,2)
)
USING DELTA
PARTITIONED BY (region)
LOCATION 'abfss://data@demodb117.dfs.core.windows.net/databricks_training/delta_partitioning_scenarios/manual_file_table';
```

```sql
INSERT INTO delta_partitioning_scenarios.cases.orders_manual_file_demo
VALUES
    (6001, 'Asha',  'NORTH', 1000.00),
    (6002, 'Rohan', 'SOUTH', 1500.00),
    (6003, 'Ira',   'WEST',  2100.00);
```

Confirm the Delta table count:

```sql
SELECT COUNT(*) AS delta_count_before_manual_copy
FROM delta_partitioning_scenarios.cases.orders_manual_file_demo;
```

Expected result:

```text
3
```

## Step 2: Create a standalone Parquet file outside the Delta table

Partition columns are commonly represented by the partition path for partitioned data files, so this staging file contains only the non-partition columns.

```python
(
    spark.sql(
        """
        SELECT
            CAST(6999 AS BIGINT) AS order_id,
            CAST('Manual Row' AS STRING) AS customer_name,
            CAST(9999.00 AS DECIMAL(12,2)) AS order_amount
        """
    )
    .coalesce(1)
    .write
    .mode("overwrite")
    .parquet(
        "abfss://data@demodb117.dfs.core.windows.net/"
        "databricks_training/delta_partitioning_scenarios/manual_file_staging"
    )
)
```

Copy the generated Parquet file directly into the Delta table root:

```python
dbutils.fs.cp(
    [
        file.path
        for file in dbutils.fs.ls(
            "abfss://data@demodb117.dfs.core.windows.net/"
            "databricks_training/delta_partitioning_scenarios/manual_file_staging"
        )
        if file.path.endswith(".parquet")
    ][0],
    "abfss://data@demodb117.dfs.core.windows.net/"
    "databricks_training/delta_partitioning_scenarios/"
    "manual_file_table/rogue_manual_file.parquet"
)

print("A Parquet file was manually copied into the Delta table root.")
```

Verify that the physical file exists:

```python
display(
    dbutils.fs.ls(
        "abfss://data@demodb117.dfs.core.windows.net/"
        "databricks_training/delta_partitioning_scenarios/manual_file_table"
    )
)
```

## Step 3: Query the Delta table again

```sql
SELECT COUNT(*) AS delta_count_after_manual_copy
FROM delta_partitioning_scenarios.cases.orders_manual_file_demo;
```

Expected result:

```text
3
```

The manually copied row is not part of the Delta table because no Delta transaction added that file.

Because the file is untracked, it should not be treated as durable table data. An unreferenced file can also become eligible for cleanup by `VACUUM` after the applicable retention period.

Inspect the files tracked by Delta:

```python
display(
    spark.read
    .json(
        "abfss://data@demodb117.dfs.core.windows.net/"
        "databricks_training/delta_partitioning_scenarios/"
        "manual_file_table/_delta_log/*.json"
    )
    .where("add.path IS NOT NULL")
    .select("add.path", "add.partitionValues")
)
```

The `rogue_manual_file.parquet` file should not appear in the Delta `add` actions.

## Important Rule

Do not use the following techniques to try to register the rogue file into a Delta table:

```text
MSCK REPAIR TABLE
ALTER TABLE ADD PARTITION
manual filesystem copying
```

Delta automatically tracks partitions through its transaction log, and manual partition-management commands are not supported for Delta tables.

The correct solution is to write the record through a Delta operation such as:

- `INSERT`
- DataFrame `.write.format("delta")`
- `MERGE`
- another supported Delta write operation

## Interview Takeaway

**Question:** What happens if someone manually copies a Parquet file into a Delta table directory?

**Answer:** The file is not part of the Delta table because it is not referenced by the transaction log. It remains an untracked physical file. Manual file operations should never be used to modify a Delta table.

---

# Scenario 6: UPDATE Changes the Partition-Column Value

## Situation

A table is partitioned by `region`.

One record currently belongs to:

```text
region = SOUTH
```

Later, the record is corrected to:

```text
region = WEST
```

What happens?

Delta can handle the change through a normal `UPDATE`. The affected data is rewritten transactionally and the row becomes part of the new logical partition.

## Create an independent table

```sql
CREATE TABLE delta_partitioning_scenarios.cases.orders_partition_update
(
    order_id BIGINT,
    customer_name STRING,
    region STRING,
    order_amount DECIMAL(12,2)
)
USING DELTA
PARTITIONED BY (region)
LOCATION 'abfss://data@demodb117.dfs.core.windows.net/databricks_training/delta_partitioning_scenarios/update_partition_column';
```

```sql
INSERT INTO delta_partitioning_scenarios.cases.orders_partition_update
VALUES
    (7001, 'Anaya', 'NORTH', 1200.00),
    (7002, 'Dev',   'SOUTH', 1800.00),
    (7003, 'Sara',  'SOUTH', 2400.00),
    (7004, 'Veer',  'WEST',  3300.00);
```

Check the record before the update:

```sql
SELECT *
FROM delta_partitioning_scenarios.cases.orders_partition_update
WHERE order_id = 7002;
```

Update the partition column:

```sql
UPDATE delta_partitioning_scenarios.cases.orders_partition_update
SET region = 'WEST'
WHERE order_id = 7002;
```

Check the record after the update:

```sql
SELECT *
FROM delta_partitioning_scenarios.cases.orders_partition_update
WHERE order_id = 7002;
```

Inspect the active partitions:

```sql
SHOW PARTITIONS delta_partitioning_scenarios.cases.orders_partition_update;
```

Because another row still uses `SOUTH`, that partition remains active.

Inspect the table history:

```sql
DESCRIBE HISTORY delta_partitioning_scenarios.cases.orders_partition_update;
```

## Interview Takeaway

**Question:** Can an `UPDATE` change a partition-column value in Delta Lake?

**Answer:** Yes. Delta rewrites the required files transactionally and the row moves logically from one partition value to another. Do not move files manually between partition directories.

---

# Scenario 7: High-Cardinality Partition Column

## Situation

A developer chooses `customer_id` as the partition column.

Every customer has a different ID.

This creates many tiny partitions.

## Create an independent table

```sql
CREATE TABLE delta_partitioning_scenarios.cases.orders_partitioned_by_customer
(
    order_id BIGINT,
    customer_id INT,
    order_amount DECIMAL(12,2)
)
USING DELTA
PARTITIONED BY (customer_id)
LOCATION 'abfss://data@demodb117.dfs.core.windows.net/databricks_training/delta_partitioning_scenarios/high_cardinality_partition';
```

Insert eight records with eight different customer IDs:

```sql
INSERT INTO delta_partitioning_scenarios.cases.orders_partitioned_by_customer
VALUES
    (8001, 10001,  500.00),
    (8002, 10002,  750.00),
    (8003, 10003, 1200.00),
    (8004, 10004, 1800.00),
    (8005, 10005, 2200.00),
    (8006, 10006, 2750.00),
    (8007, 10007, 3100.00),
    (8008, 10008, 3900.00);
```

Inspect the partitions:

```sql
SHOW PARTITIONS delta_partitioning_scenarios.cases.orders_partitioned_by_customer;
```

You should observe eight logical partitions for only eight records.

This is deliberately exaggerated to show the problem.

```text
8 records
    ↓
8 customer IDs
    ↓
8 partitions
    ↓
Poor partition granularity
```

In a production table with millions of customers, the number of partitions could become extremely large.

## Interview Takeaway

**Question:** Why is `customer_id` usually a poor traditional partition column?

**Answer:** It has high cardinality. High-cardinality partitioning can create huge numbers of partitions and small files, increasing metadata and maintenance overhead. Columns such as date or region are traditionally safer only when each partition contains substantial data.

---

# Scenario 8: Late-Arriving Data for an Existing Old Partition

## Situation

A table is partitioned by `order_date`.

The table currently contains data for January and February. In March, a late record arrives for January.

Do we need to manually recreate or register the January partition?

No. Delta automatically tracks partitions as data is written.

## Create an independent table

```sql
CREATE TABLE delta_partitioning_scenarios.cases.late_arriving_orders
(
    order_id BIGINT,
    customer_name STRING,
    order_date DATE,
    order_amount DECIMAL(12,2)
)
USING DELTA
PARTITIONED BY (order_date)
LOCATION 'abfss://data@demodb117.dfs.core.windows.net/databricks_training/delta_partitioning_scenarios/late_arriving_orders';
```

Insert the initial data:

```sql
INSERT INTO delta_partitioning_scenarios.cases.late_arriving_orders
VALUES
    (9001, 'Arjun', DATE '2026-01-10', 1200.00),
    (9002, 'Diya',  DATE '2026-02-12', 1800.00);
```

Later, insert an old January record:

```sql
INSERT INTO delta_partitioning_scenarios.cases.late_arriving_orders
VALUES
    (9003, 'Neel', DATE '2026-01-10', 2400.00);
```

Query the January partition:

```sql
SELECT *
FROM delta_partitioning_scenarios.cases.late_arriving_orders
WHERE order_date = DATE '2026-01-10'
ORDER BY order_id;
```

Expected records:

```text
9001
9003
```

## Interview Takeaway

**Question:** Do we need `ALTER TABLE ADD PARTITION` when late-arriving Delta data belongs to an old partition?

**Answer:** No. Delta automatically updates its partition metadata through the transaction log when the new data is written.

---

# Additional Interview Questions

## Q1. Can we use `MSCK REPAIR TABLE` to discover manually added Delta partition folders?

No. Delta tables automatically track active files and partition values through the transaction log. Traditional partition-recovery commands are not used to manage Delta partitions.

---

## Q2. Can two Delta transactions be restricted to separate partitions and therefore never conflict?

Do not assume partition boundaries are transaction boundaries. Delta transactions operate at the table level, and concurrency rules depend on the files and rows affected by the operations.

---

## Q3. If all rows from one partition are deleted, is the old physical data immediately removed?

No. The delete creates a new Delta table state in which the old files are no longer active. Old physical files can remain until they become eligible for `VACUUM`.

---

## Q4. Can we directly query a Delta partition directory instead of filtering the table?

Do not depend on physical partition directories for Delta table access. Query the Delta table and use a partition filter:

```sql
SELECT *
FROM some_delta_table
WHERE partition_column = some_value;
```

The transaction log is the source of truth for the table.

---

## Q5. What if an existing Delta location is registered with a different schema or partition definition?

The provided definition must match the metadata stored in the Delta location. A mismatch should be treated as an error instead of redefining the existing data layout.

---

## Q6. Is a directory with `year=2026/month=01` automatically a Delta table?

No. Partition-style folder names only describe a physical layout. A Delta table additionally requires a valid Delta transaction log.

```text
Partition folders only
    → Could be Parquet, CSV, ORC, or another format

Partition folders + valid _delta_log
    → Delta table
```

---

## Q7. If a rogue Parquet file is manually added inside an existing partition folder instead of the table root, will Delta read it?

No. The location of the rogue file does not make it part of the Delta table. It must be referenced by a committed Delta transaction.

---

## Q8. Can changing a partition strategy be expensive?

Yes. Traditional partitioning determines the physical organization of the table. Changing that strategy normally means rewriting a large amount of data into a new layout.

---

# Final Decision Summary

| Situation | Correct approach |
|---|---|
| Need to change partition column | Rewrite into a new table/layout and validate |
| Partition column contains `NULL` | Allowed when nullable; query using `IS NULL` |
| Existing location already contains Delta | Register with `USING DELTA LOCATION`; let Delta metadata define the layout |
| Existing location contains partitioned Parquet | Convert to Delta or register as Parquet first |
| Parquet file copied manually into Delta location | File is untracked and invisible to Delta |
| `UPDATE` changes partition value | Use normal Delta `UPDATE`; Delta rewrites affected data transactionally |
| High-cardinality partition column | Usually avoid traditional partitioning |
| Late data belongs to an old partition | Insert normally; Delta tracks the partition automatically |
| Need to recover manually added Delta partitions | Do not use `MSCK REPAIR`; write through Delta APIs |

---

# Key Rule to Remember

```text
For Delta Lake:

The transaction log is the source of truth.

Physical folders alone do not decide
which files or partitions belong to the table.
```

---

# Optional Cleanup

Run this only after all exercises are complete.

```python
spark.sql("DROP CATALOG IF EXISTS delta_partitioning_scenarios CASCADE")

dbutils.fs.rm(
    "abfss://data@demodb117.dfs.core.windows.net/"
    "databricks_training/delta_partitioning_scenarios",
    True
)

print("Scenario metadata and files were removed.")
```
