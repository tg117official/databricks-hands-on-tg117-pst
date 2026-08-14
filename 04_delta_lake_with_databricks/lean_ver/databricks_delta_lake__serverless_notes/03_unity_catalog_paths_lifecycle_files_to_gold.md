# Unity Catalog Paths, Object Lifecycle, and Files-to-Gold POC

> Run the Python cells on Databricks serverless notebook compute. Direct `abfss://` operations require `READ FILES` and `WRITE FILES` on the external location. The examples use `demodb117/data` and dedicated session directories so reruns do not affect unrelated data.


### 1.5-Hour Hands-on Guide

## 2. Complete Workflow

```mermaid
flowchart TD
    A[Inspect managed storage inheritance]
    A --> B[Create managed volume]
    B --> C[Upload CSV and JSON source files]

    C --> D1[Create managed Delta table from CSV]
    C --> D2[Create managed Delta table from JSON]
    C --> D3[Create managed Delta table with INSERT INTO]

    C --> E1[Prepare external CSV directory]
    C --> E2[Prepare external JSON directory]
    C --> E3[Prepare external Delta directory]

    E1 --> F1[Register external CSV table]
    E2 --> F2[Register external JSON table]
    E3 --> F3[Register external Delta table]

    D1 --> G[Compare table paths]
    D2 --> G
    D3 --> G
    F1 --> G
    F2 --> G
    F3 --> G

    G --> H[Test path overlap]
    H --> I[Drop and recover managed table]
    I --> J[Drop and re-register external table]

    J --> K[Query all source tables]
    K --> L[Combine and standardize records]
    L --> M[Create gold_daily_city_sales]
```

## 6. Source Table Design

All source tables use the same columns:

Using the same structure makes the final `UNION ALL` easy to understand.

## Part A — Set Up the Namespace

## 9. Select the Catalog

Run in a SQL cell:

```sql
USE CATALOG training_catalog;
```

Check the current catalog:

```sql
SELECT current_catalog();
```

## 10. Create the Schema

```sql
CREATE SCHEMA IF NOT EXISTS training_catalog.session_demo
COMMENT 'Schema used for paths, lifecycle, and files-to-gold POC';
```

Select it:

```sql
USE SCHEMA session_demo;
```

Check the namespace:

```sql
SELECT
    current_catalog() AS current_catalog,
    current_schema() AS current_schema;
```

## Part B — Inspect Managed-Storage Inheritance

## 11. Why Managed-Storage Inheritance Matters

A managed table or managed volume does not require you to provide a physical path.

Unity Catalog selects the location.

## 12. Inspect the Catalog

```sql
DESCRIBE CATALOG EXTENDED training_catalog;
```

Look for storage-related information.

Depending on the workspace configuration, the output can show information such as:

## 13. Inspect the Schema

```sql
DESCRIBE SCHEMA EXTENDED training_catalog.session_demo;
```

Ask these questions while reviewing the output:

## 14. Expected Storage Decision

You do not manually create the internal managed table directories.

Unity Catalog creates unique internal paths for managed tables and managed volumes.

## Part C — Create the Managed Volume

## 15. Create the Volume

```sql
CREATE VOLUME IF NOT EXISTS
    training_catalog.session_demo.source_files
COMMENT 'Source files used in the files-to-gold POC';
```

Inspect it:

```sql
DESCRIBE VOLUME
    training_catalog.session_demo.source_files;
```

The volume path is:

## 16. Create Four Source Directories

The volume will contain four source areas:

The first two files will create managed Delta tables.

## Generate all four input files

Run this Python cell after creating `training_catalog.session_demo.source_files`:

```python
files = {
    "/Volumes/training_catalog/session_demo/source_files/managed_csv/managed_orders.csv": """order_id,customer_name,city,product_category,order_amount,order_date
101,Aditi,Pune,Electronics,1250.00,2026-07-20
102,Rahul,Mumbai,Books,650.00,2026-07-20
103,Neha,Bengaluru,Home,2100.00,2026-07-21
104,Aman,Pune,Fitness,1800.00,2026-07-21
""",
    "/Volumes/training_catalog/session_demo/source_files/managed_json/managed_orders.json": """{"order_id":201,"customer_name":"Priya","city":"Pune","product_category":"Books","order_amount":900.00,"order_date":"2026-07-22"}
{"order_id":202,"customer_name":"Vikram","city":"Mumbai","product_category":"Electronics","order_amount":3200.00,"order_date":"2026-07-22"}
{"order_id":203,"customer_name":"Meera","city":"Bengaluru","product_category":"Home","order_amount":1450.00,"order_date":"2026-07-23"}
{"order_id":204,"customer_name":"Karan","city":"Pune","product_category":"Fitness","order_amount":1100.00,"order_date":"2026-07-23"}
""",
    "/Volumes/training_catalog/session_demo/source_files/external_csv/external_orders.csv": """order_id,customer_name,city,product_category,order_amount,order_date
401,Riya,Mumbai,Electronics,2700.00,2026-07-24
402,Kabir,Pune,Home,1600.00,2026-07-24
403,Isha,Bengaluru,Books,750.00,2026-07-25
404,Arjun,Mumbai,Fitness,2300.00,2026-07-25
""",
    "/Volumes/training_catalog/session_demo/source_files/external_json/external_orders.json": """{"order_id":501,"customer_name":"Sana","city":"Pune","product_category":"Electronics","order_amount":1850.00,"order_date":"2026-07-26"}
{"order_id":502,"customer_name":"Mohit","city":"Mumbai","product_category":"Home","order_amount":2900.00,"order_date":"2026-07-26"}
{"order_id":503,"customer_name":"Anaya","city":"Bengaluru","product_category":"Fitness","order_amount":1250.00,"order_date":"2026-07-27"}
{"order_id":504,"customer_name":"Dev","city":"Pune","product_category":"Books","order_amount":800.00,"order_date":"2026-07-27"}
""",
}

for path, content in files.items():
    dbutils.fs.mkdirs(path.rsplit("/", 1)[0])
    dbutils.fs.put(path, content, True)

display(dbutils.fs.ls("/Volumes/training_catalog/session_demo/source_files"))
```

## 22. Verify the Uploads

Run in a Python cell:

```python
base_volume_path = (
    "/Volumes/training_catalog/"
    "session_demo/source_files"
)

for folder in [
    "managed_csv",
    "managed_json",
    "external_csv",
    "external_json",
]:
    print(f"\nFiles in {folder}:")
    display(
        dbutils.fs.ls(
            f"{base_volume_path}/{folder}/"
        )
    )
```

Expected files:

## Part E — Create Managed Delta Source Tables

## 23. Important Format Distinction

The CSV and JSON files are source formats.

The managed tables created from them will be Delta tables.

## 24. Create a Managed Delta Table from CSV

Use `read_files` with an explicit schema:

```sql
CREATE OR REPLACE TABLE
    training_catalog.session_demo.managed_csv_orders
USING DELTA
COMMENT 'Managed Delta table created from a CSV source file'
AS
SELECT
    order_id,
    customer_name,
    city,
    product_category,
    order_amount,
    order_date
FROM read_files(
    '/Volumes/training_catalog/session_demo/source_files/managed_csv/',
    format => 'csv',
    header => true,
    schema => '
        order_id INT,
        customer_name STRING,
        city STRING,
        product_category STRING,
        order_amount DECIMAL(10,2),
        order_date DATE
    '
);
```

No `LOCATION` is provided.

## 25. Verify the CSV-Origin Managed Table

```sql
SELECT *
FROM training_catalog.session_demo.managed_csv_orders
ORDER BY order_id;
```

Expected count:

```sql
SELECT COUNT(*) AS row_count
FROM training_catalog.session_demo.managed_csv_orders;
```

Expected result:

```sql
DESCRIBE DETAIL
training_catalog.session_demo.managed_csv_orders;
```

## 26. Create a Managed Delta Table from JSON

```sql
CREATE OR REPLACE TABLE
    training_catalog.session_demo.managed_json_orders
USING DELTA
COMMENT 'Managed Delta table created from a JSON source file'
AS
SELECT
    order_id,
    customer_name,
    city,
    product_category,
    order_amount,
    order_date
FROM read_files(
    '/Volumes/training_catalog/session_demo/source_files/managed_json/',
    format => 'json',
    schema => '
        order_id INT,
        customer_name STRING,
        city STRING,
        product_category STRING,
        order_amount DECIMAL(10,2),
        order_date DATE
    '
);
```

## 27. Verify the JSON-Origin Managed Table

```sql
SELECT *
FROM training_catalog.session_demo.managed_json_orders
ORDER BY order_id;
```

Expected count:

```sql
SELECT COUNT(*) AS row_count
FROM training_catalog.session_demo.managed_json_orders;
```

Expected result:

```sql
DESCRIBE DETAIL
training_catalog.session_demo.managed_json_orders;
```

## 28. Create the Managed Delta Source Table

Create an empty managed Delta table:

```sql
CREATE OR REPLACE TABLE
    training_catalog.session_demo.managed_delta_orders
(
    order_id          INT,
    customer_name     STRING,
    city              STRING,
    product_category  STRING,
    order_amount      DECIMAL(10,2),
    order_date        DATE
)
USING DELTA
COMMENT 'Managed Delta source table populated using INSERT INTO';
```

No `LOCATION` is supplied.

## 29. Insert Eight Rows

```sql
INSERT INTO training_catalog.session_demo.managed_delta_orders
VALUES
    (301, 'Anil',   'Mumbai',    'Books',       700.00,  DATE '2026-07-20'),
    (302, 'Pooja',  'Pune',      'Electronics', 2600.00, DATE '2026-07-20'),
    (303, 'Rohan',  'Bengaluru', 'Fitness',     1500.00, DATE '2026-07-21'),
    (304, 'Kavya',  'Mumbai',    'Home',        1950.00, DATE '2026-07-21'),
    (305, 'Nitin',  'Pune',      'Books',       550.00,  DATE '2026-07-22'),
    (306, 'Simran', 'Bengaluru', 'Electronics', 3100.00, DATE '2026-07-22'),
    (307, 'Varun',  'Mumbai',    'Fitness',     2250.00, DATE '2026-07-23'),
    (308, 'Diya',   'Pune',      'Home',        1750.00, DATE '2026-07-23');
```

## 30. Verify the Managed Delta Source Table

```sql
SELECT *
FROM training_catalog.session_demo.managed_delta_orders
ORDER BY order_id;
```

Expected count:

```sql
SELECT COUNT(*) AS row_count
FROM training_catalog.session_demo.managed_delta_orders;
```

Expected result:

```sql
DESCRIBE DETAIL
training_catalog.session_demo.managed_delta_orders;
```

## 31. Managed Source Architecture

All three tables are managed Delta tables.

Their sources are different, but their final table format is Delta.

## Part F — Prepare External Table Data

## 32. Why the Volume Cannot Be the External Table Location

Files in a Unity Catalog volume cannot be registered in place as Unity Catalog tables.

Volumes are intended for path-based file access.

## 33. Define the External Paths

Run in a Python cell.

Replace the placeholders first:

```python
external_root = (
    "abfss://data@demodb117.dfs.core.windows.net/uc_session_demo"
)

external_csv_path = (
    f"{external_root}/external_csv_orders"
)

external_json_path = (
    f"{external_root}/external_json_orders"
)

external_delta_path = (
    f"{external_root}/external_delta_orders"
)

print(external_csv_path)
print(external_json_path)
print(external_delta_path)
```

## 34. Define the Shared Schema

```python
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DecimalType,
    DateType,
)

order_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("product_category", StringType(), True),
    StructField("order_amount", DecimalType(10, 2), True),
    StructField("order_date", DateType(), True),
])
```

## 35. Read the External CSV Staging File

```python
external_csv_df = (
    spark.read
    .schema(order_schema)
    .option("header", True)
    .csv(
        "/Volumes/training_catalog/session_demo/"
        "source_files/external_csv/"
    )
)

display(external_csv_df)
```

## 36. Write CSV Data to ADLS

```python
(
    external_csv_df.write
    .mode("overwrite")
    .option("header", True)
    .csv(external_csv_path)
)

print(
    f"CSV data written to: {external_csv_path}"
)
```

Spark writes a directory containing one or more CSV part files.

## 37. Read the External JSON Staging File

```python
external_json_df = (
    spark.read
    .schema(order_schema)
    .json(
        "/Volumes/training_catalog/session_demo/"
        "source_files/external_json/"
    )
)

display(external_json_df)
```

## 38. Write JSON Data to ADLS

```python
(
    external_json_df.write
    .mode("overwrite")
    .json(external_json_path)
)

print(
    f"JSON data written to: {external_json_path}"
)
```

## 39. Prepare External Delta Data

```python
from decimal import Decimal
from datetime import date

external_delta_rows = [
    (
        601,
        "Tara",
        "Mumbai",
        "Books",
        Decimal("950.00"),
        date(2026, 7, 28),
    ),
    (
        602,
        "Yash",
        "Pune",
        "Home",
        Decimal("2050.00"),
        date(2026, 7, 28),
    ),
    (
        603,
        "Maya",
        "Bengaluru",
        "Electronics",
        Decimal("3400.00"),
        date(2026, 7, 29),
    ),
    (
        604,
        "Om",
        "Mumbai",
        "Fitness",
        Decimal("1700.00"),
        date(2026, 7, 29),
    ),
]

external_delta_df = spark.createDataFrame(
    external_delta_rows,
    schema=order_schema,
)

display(external_delta_df)
```

## 40. Write Delta Data to ADLS

```python
(
    external_delta_df.write
    .format("delta")
    .mode("overwrite")
    .save(external_delta_path)
)

print(
    f"Delta data written to: {external_delta_path}"
)
```

## 41. Verify the Three External Directories

```python
for name, path in {
    "CSV": external_csv_path,
    "JSON": external_json_path,
    "DELTA": external_delta_path,
}.items():
    print(f"\n{name} directory: {path}")
    display(dbutils.fs.ls(path))
```

Expected:

## Part G — Register External Tables

## 42. Create the External CSV Table

Replace the location with your actual path:

```sql
CREATE TABLE
    training_catalog.session_demo.external_csv_orders
(
    order_id          INT,
    customer_name     STRING,
    city              STRING,
    product_category  STRING,
    order_amount      DECIMAL(10,2),
    order_date        DATE
)
USING CSV
OPTIONS (
    header = 'true'
)
LOCATION
'abfss://data@demodb117.dfs.core.windows.net/uc_session_demo/external_csv_orders'
COMMENT 'External table backed by CSV files';
```

The schema is provided because CSV files do not store a reliable table schema.

## 43. Verify the External CSV Table

```sql
SELECT *
FROM training_catalog.session_demo.external_csv_orders
ORDER BY order_id;
```

Expected count:

```sql
SELECT COUNT(*) AS row_count
FROM training_catalog.session_demo.external_csv_orders;
```

Expected result:

## 44. Create the External JSON Table

```sql
CREATE TABLE
    training_catalog.session_demo.external_json_orders
(
    order_id          INT,
    customer_name     STRING,
    city              STRING,
    product_category  STRING,
    order_amount      DECIMAL(10,2),
    order_date        DATE
)
USING JSON
LOCATION
'abfss://data@demodb117.dfs.core.windows.net/uc_session_demo/external_json_orders'
COMMENT 'External table backed by JSON files';
```

## 45. Verify the External JSON Table

```sql
SELECT *
FROM training_catalog.session_demo.external_json_orders
ORDER BY order_id;
```

Expected count:

```sql
SELECT COUNT(*) AS row_count
FROM training_catalog.session_demo.external_json_orders;
```

Expected result:

## 46. Create the External Delta Table

The Delta directory already contains a Delta transaction log.

Register it:

```sql
CREATE TABLE
    training_catalog.session_demo.external_delta_orders
USING DELTA
LOCATION
'abfss://data@demodb117.dfs.core.windows.net/uc_session_demo/external_delta_orders'
COMMENT 'External Delta source table';
```

## 47. Verify the External Delta Table

```sql
SELECT *
FROM training_catalog.session_demo.external_delta_orders
ORDER BY order_id;
```

Expected count:

```sql
SELECT COUNT(*) AS row_count
FROM training_catalog.session_demo.external_delta_orders;
```

Expected result:

## 48. External Source Architecture

## Part H — Compare Managed and External Paths

## 49. Inspect the Managed CSV-Origin Table

```sql
DESCRIBE DETAIL
training_catalog.session_demo.managed_csv_orders;
```

Observe:

## 50. Inspect the Managed JSON-Origin Table

```sql
DESCRIBE DETAIL
training_catalog.session_demo.managed_json_orders;
```

Observe:

## 51. Inspect the Managed Delta Source Table

```sql
DESCRIBE DETAIL
training_catalog.session_demo.managed_delta_orders;
```

Observe:

## 52. Inspect the External Delta Table

```sql
DESCRIBE DETAIL
training_catalog.session_demo.external_delta_orders;
```

Observe:

`DESCRIBE DETAIL` is most useful for Delta tables.

## 53. Managed Versus External Comparison

| Table | Source | Final format | Who selected the table path? |
|---|---|---|---|
| `managed_csv_orders` | CSV file | Delta | Unity Catalog |
| `managed_json_orders` | JSON file | Delta | Unity Catalog |
| `managed_delta_orders` | SQL rows | Delta | Unity Catalog |
| `external_csv_orders` | CSV directory | CSV | You |
| `external_json_orders` | JSON directory | JSON | You |
| `external_delta_orders` | Delta directory | Delta | You |
## 54. The Key Difference

## Part I — Demonstrate Path-Overlap Protection

## 55. One Path Must Belong to One Governed Object

Unity Catalog prevents overlapping table and volume paths.

Examples of invalid designs:

## 56. Attempt to Register a Second Table on the CSV Path

Run:

```sql
CREATE TABLE
    training_catalog.session_demo.invalid_csv_copy
(
    order_id          INT,
    customer_name     STRING,
    city              STRING,
    product_category  STRING,
    order_amount      DECIMAL(10,2),
    order_date        DATE
)
USING CSV
OPTIONS (
    header = 'true'
)
LOCATION
'abfss://data@demodb117.dfs.core.windows.net/uc_session_demo/external_csv_orders';
```

Expected result:

## 57. Confirm That the Invalid Table Was Not Created

```sql
SHOW TABLES IN training_catalog.session_demo;
```

You should not see:

## 58. Path-Overlap Flow

## 59. Correct Path Design

Each table has a separate directory.

## Part J — Managed Table Lifecycle

## 60. Use the Managed Delta Source Table

The lifecycle demonstration uses:

Check its data:

```sql
SELECT *
FROM training_catalog.session_demo.managed_delta_orders
ORDER BY order_id;
```

## 61. Drop the Managed Table

```sql
DROP TABLE
training_catalog.session_demo.managed_delta_orders;
```

Confirm that it is gone:

```sql
SHOW TABLES IN training_catalog.session_demo;
```

Try to query it:

```sql
SELECT *
FROM training_catalog.session_demo.managed_delta_orders;
```

## 62. Recover the Managed Table

```sql
UNDROP TABLE
training_catalog.session_demo.managed_delta_orders;
```

Query it again:

```sql
SELECT *
FROM training_catalog.session_demo.managed_delta_orders
ORDER BY order_id;
```

Expected result:

## 63. Managed Lifecycle Flow

By default, eligible Unity Catalog tables can normally be recovered during the configured recovery period.

The parent catalog and schema must still exist.

## 64. Verify the Recovered Table

```sql
SELECT COUNT(*) AS recovered_rows
FROM training_catalog.session_demo.managed_delta_orders;
```

Expected result:

## Part K — External Table Lifecycle

## 65. Use the External CSV Table

Check it before dropping:

```sql
SELECT *
FROM training_catalog.session_demo.external_csv_orders
ORDER BY order_id;
```

Expected count:

## 66. Drop the External Table

```sql
DROP TABLE
training_catalog.session_demo.external_csv_orders;
```

Confirm that its metadata is removed:

```sql
SHOW TABLES IN training_catalog.session_demo;
```

The table name should no longer be listed.

## 67. Confirm That the CSV Files Still Exist

Run in Python:

```python
external_csv_path = (
    "abfss://data@demodb117.dfs.core.windows.net/uc_session_demo/"
    "external_csv_orders"
)

display(
    dbutils.fs.ls(external_csv_path)
)
```

Expected result:

## 68. Re-register the External CSV Table

```sql
CREATE TABLE
    training_catalog.session_demo.external_csv_orders
(
    order_id          INT,
    customer_name     STRING,
    city              STRING,
    product_category  STRING,
    order_amount      DECIMAL(10,2),
    order_date        DATE
)
USING CSV
OPTIONS (
    header = 'true'
)
LOCATION
'abfss://data@demodb117.dfs.core.windows.net/uc_session_demo/external_csv_orders'
COMMENT 'Re-registered external CSV source table';
```

No data is copied.

The existing files are registered again.

## 69. Verify the Re-registered Table

```sql
SELECT *
FROM training_catalog.session_demo.external_csv_orders
ORDER BY order_id;
```

Expected result:

## 70. External Lifecycle Flow

## 71. Why Re-registration Is Useful Here

Unity Catalog can support `UNDROP` for eligible managed and external relations.

This POC uses re-registration for the external CSV table because it clearly shows:

## Part L — Query the Six Source Tables

## 72. Verify Every Source Count

```sql
SELECT
    'managed_csv_orders' AS table_name,
    COUNT(*) AS row_count
FROM training_catalog.session_demo.managed_csv_orders

UNION ALL

SELECT
    'managed_json_orders',
    COUNT(*)
FROM training_catalog.session_demo.managed_json_orders

UNION ALL

SELECT
    'managed_delta_orders',
    COUNT(*)
FROM training_catalog.session_demo.managed_delta_orders

UNION ALL

SELECT
    'external_csv_orders',
    COUNT(*)
FROM training_catalog.session_demo.external_csv_orders

UNION ALL

SELECT
    'external_json_orders',
    COUNT(*)
FROM training_catalog.session_demo.external_json_orders

UNION ALL

SELECT
    'external_delta_orders',
    COUNT(*)
FROM training_catalog.session_demo.external_delta_orders;
```

Expected counts:

| Table | Expected rows |
|---|---:|
| `managed_csv_orders` | 4 |
| `managed_json_orders` | 4 |
| `managed_delta_orders` | 8 |
| `external_csv_orders` | 4 |
| `external_json_orders` | 4 |
| `external_delta_orders` | 4 |
| **Total** | **28** |
## 73. Why `UNION ALL` Is Used

`UNION ALL` keeps every source record.

The source tables use different order ID ranges, so all 28 rows are expected to remain.

## Part M — Combine and Standardize the Source Data

## 74. Create a Temporary Combined View

```sql
CREATE OR REPLACE TEMP VIEW all_order_sources
AS

SELECT
    order_id,
    customer_name,
    city,
    product_category,
    CAST(order_amount AS DECIMAL(10,2))
        AS order_amount,
    CAST(order_date AS DATE)
        AS order_date,
    'managed_csv' AS source_name
FROM training_catalog.session_demo.managed_csv_orders

UNION ALL

SELECT
    order_id,
    customer_name,
    city,
    product_category,
    CAST(order_amount AS DECIMAL(10,2)),
    CAST(order_date AS DATE),
    'managed_json'
FROM training_catalog.session_demo.managed_json_orders

UNION ALL

SELECT
    order_id,
    customer_name,
    city,
    product_category,
    CAST(order_amount AS DECIMAL(10,2)),
    CAST(order_date AS DATE),
    'managed_delta'
FROM training_catalog.session_demo.managed_delta_orders

UNION ALL

SELECT
    order_id,
    customer_name,
    city,
    product_category,
    CAST(order_amount AS DECIMAL(10,2)),
    CAST(order_date AS DATE),
    'external_csv'
FROM training_catalog.session_demo.external_csv_orders

UNION ALL

SELECT
    order_id,
    customer_name,
    city,
    product_category,
    CAST(order_amount AS DECIMAL(10,2)),
    CAST(order_date AS DATE),
    'external_json'
FROM training_catalog.session_demo.external_json_orders

UNION ALL

SELECT
    order_id,
    customer_name,
    city,
    product_category,
    CAST(order_amount AS DECIMAL(10,2)),
    CAST(order_date AS DATE),
    'external_delta'
FROM training_catalog.session_demo.external_delta_orders;
```

## 75. Query the Combined View

```sql
SELECT *
FROM all_order_sources
ORDER BY order_id;
```

Expected row count:

```sql
SELECT COUNT(*) AS total_rows
FROM all_order_sources;
```

Expected result:

## 76. Check Source Distribution

```sql
SELECT
    source_name,
    COUNT(*) AS total_orders,
    ROUND(SUM(order_amount), 2)
        AS total_sales
FROM all_order_sources
GROUP BY source_name
ORDER BY source_name;
```

This confirms that every source contributed records.

## 77. Check for Duplicate Order IDs

```sql
SELECT
    order_id,
    COUNT(*) AS duplicate_count
FROM all_order_sources
GROUP BY order_id
HAVING COUNT(*) > 1;
```

Expected result:

## 78. Check for Missing Important Values

```sql
SELECT *
FROM all_order_sources
WHERE
    order_id IS NULL
    OR city IS NULL
    OR order_amount IS NULL
    OR order_date IS NULL;
```

Expected result:

## 79. Standardization Flow

## Part N — Create the Gold-Layer Table

## 80. What Makes the Destination Look Like Gold Data

A gold-layer table should answer a business question.

Instead of storing every detailed order again, the destination table will summarize:

## 81. Create the Managed Gold Delta Table

```sql
CREATE OR REPLACE TABLE
    training_catalog.session_demo.gold_daily_city_sales
USING DELTA
COMMENT 'Daily city-level sales summary created from all managed and external order sources'
AS
SELECT
    order_date,
    city,
    COUNT(*) AS total_orders,
    ROUND(
        SUM(order_amount),
        2
    ) AS total_sales,
    ROUND(
        AVG(order_amount),
        2
    ) AS average_order_amount,
    COUNT(
        DISTINCT source_name
    ) AS source_type_count,
    CURRENT_TIMESTAMP() AS refreshed_at
FROM all_order_sources
GROUP BY
    order_date,
    city;
```

No `LOCATION` is supplied.

Therefore:

## 82. Query the Gold Table

```sql
SELECT *
FROM training_catalog.session_demo.gold_daily_city_sales
ORDER BY
    order_date,
    city;
```

## 83. Validate the Gold Totals

Check the number of detailed orders represented by the gold table:

```sql
SELECT
    SUM(total_orders) AS represented_orders,
    ROUND(
        SUM(total_sales),
        2
    ) AS represented_sales
FROM training_catalog.session_demo.gold_daily_city_sales;
```

Expected result:

## 84. Inspect the Gold Table

```sql
DESCRIBE DETAIL
training_catalog.session_demo.gold_daily_city_sales;
```

Focus on:

Expected format:

## 85. Final Architecture

## 86. Layered View

## Part O — Key Comparisons

## 87. File Format Versus Table Type

| Source file | Created table | Table type |
|---|---|---|
| CSV | `managed_csv_orders` | Managed Delta |
| JSON | `managed_json_orders` | Managed Delta |
| SQL rows | `managed_delta_orders` | Managed Delta |
| CSV directory | `external_csv_orders` | External CSV |
| JSON directory | `external_json_orders` | External JSON |
| Delta directory | `external_delta_orders` | External Delta |
## 88. Managed Versus External Lifecycle

| Behaviour | Managed table | External table |
|---|---|---|
| Path selected by | Unity Catalog | You |
| `LOCATION` supplied | No | Yes |
| Metadata managed by Unity Catalog | Yes | Yes |
| File lifecycle managed by Unity Catalog | Yes | No |
| Drop removes registration | Yes | Yes |
| External files remain after drop | Not applicable | Yes |
| Recovery pattern shown | `UNDROP TABLE` | Re-register with `LOCATION` |
## 89. Volume Versus Table

| Volume | Table |
|---|---|
| Stores files | Stores governed tabular data |
| Accessed by a path | Accessed by a three-part name |
| Can contain CSV, JSON, images, or other files | Has a defined schema |
| Does not automatically turn files into tables | Supports table operations |
| Good for staging | Good for analytics and transformations |
## Part P — Common Questions

## 90. Can CSV and JSON Files Create Managed Tables?

Yes.

The files are read, and their records are written into a managed Delta table.

## 91. Can CSV and JSON Be External Tables?

Yes.

Unity Catalog external tables can directly reference CSV and JSON directories.

- A declared schema
- A `USING CSV` or `USING JSON` clause
- A unique `LOCATION`
- An approved external location
## 92. Can Two Tables Use the Same Location?

No.

Unity Catalog prevents path overlap.

```sql
CREATE OR REPLACE VIEW
    training_catalog.session_demo.external_csv_orders_view
AS
SELECT *
FROM training_catalog.session_demo.external_csv_orders;
```

## 93. Can an External Table Be Recovered with `UNDROP`?

Eligible Unity Catalog external relations can support `UNDROP`.

This POC uses re-registration to make the file-lifecycle behaviour visible.

## 94. Can a Managed CSV Table Be Created?

Use careful wording.

This is not a managed CSV table:

## 95. `CREATE TABLE` from Volume Fails

Check:

- The volume exists.
- The path includes catalog, schema, and volume.
- The file was uploaded into the correct directory.
- The compute supports Unity Catalog volumes.
- The account has `READ VOLUME`.
- The account has `CREATE TABLE` on the destination schema.
## 96. External Data Write Fails

Check:

- The ADLS path is covered by an external location.
- The managed identity can access the storage.
- The current principal has permission to write files.
- The path is not inside managed storage.
- The path does not overlap an existing table or volume.
- The placeholders were replaced.
## 97. External Table Creation Fails

Check:

- `CREATE EXTERNAL TABLE` is granted on the external location.
- `USE CATALOG` and `USE SCHEMA` are granted.
- `CREATE TABLE` is granted on the schema.
- The path is unique.
- The CSV header option is included.
- The provided schema matches the source data.
## 98. CSV Columns Are Shifted or Incorrect

Check:

- `header = 'true'`
- Delimiter
- Column order
- Number of fields
- Date format
- Decimal values
- Extra commas in text values
## 99. JSON Records Are Missing

The example uses newline-delimited JSON.

Each line must contain one complete JSON object:

## 100. `UNDROP TABLE` Fails

Check:

- The table was registered in Unity Catalog.
- The recovery period has not expired.
- The catalog still exists.
- The schema still exists.
- Another active object is not using the same name.
- The current identity has the required privileges.
## 101. Gold Count Is Not 28

Run the source-count query again.

Expected source counts:

## Part R — Quick Recap

## 102. Question 1

Which managed location takes priority?

## 103. Question 2

What is the final format of `managed_csv_orders`?

## 104. Question 3

Why can `external_csv_orders` not share a location with another table?

## 105. Question 4

What happened when `managed_delta_orders` was dropped?

## 106. Question 5

What happened to the CSV files when `external_csv_orders` was dropped?

## 107. Question 6

Why is `gold_daily_city_sales` considered a gold table?

## 108. Final Takeaways

## Part S — Optional Cleanup

## 109. Drop the Gold Table

```sql
DROP TABLE IF EXISTS
training_catalog.session_demo.gold_daily_city_sales;
```

## 110. Drop the Managed Source Tables

```sql
DROP TABLE IF EXISTS
training_catalog.session_demo.managed_csv_orders;

DROP TABLE IF EXISTS
training_catalog.session_demo.managed_json_orders;

DROP TABLE IF EXISTS
training_catalog.session_demo.managed_delta_orders;
```

Managed tables follow Unity Catalog lifecycle rules.

## 111. Drop the External Table Registrations

```sql
DROP TABLE IF EXISTS
training_catalog.session_demo.external_csv_orders;

DROP TABLE IF EXISTS
training_catalog.session_demo.external_json_orders;

DROP TABLE IF EXISTS
training_catalog.session_demo.external_delta_orders;
```

The external ADLS files remain.

Delete them separately only when they are no longer required.

## 112. Drop the Volume

```sql
DROP VOLUME IF EXISTS
training_catalog.session_demo.source_files;
```

This removes the managed volume according to Unity Catalog managed-object lifecycle rules.

## 113. Drop the Schema

Run this only when the schema contains no other required objects:

```sql
DROP SCHEMA IF EXISTS
training_catalog.session_demo
CASCADE;
```
