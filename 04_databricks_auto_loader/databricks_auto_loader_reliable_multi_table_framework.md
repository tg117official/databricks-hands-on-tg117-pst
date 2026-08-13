# Reliable Multi-Table Auto Loader Framework

This lesson builds one configuration-driven Auto Loader framework for three JSON entities: `orders`, `customers`, and `products`. Each entity has an independent landing path, checkpoint, schema, and Bronze Delta table.

The same framework is used for every scenario:

```text
Generate three input files
        ↓
Run the shared Auto Loader framework
        ↓
Validate Bronze, rescued, and corrupt records
```

## Session index

1. [Framework configuration](#1-framework-configuration)
2. [Scenario 1: valid multi-entity ingestion](#2-scenario-1-valid-multi-entity-ingestion)
3. [Scenario 2: incremental files and repeated business keys](#3-scenario-2-incremental-files-and-repeated-business-keys)
4. [Scenario 3: corrupt JSON records](#4-scenario-3-corrupt-json-records)
5. [Scenario 4: rescued data](#5-scenario-4-rescued-data)
6. [Final reconciliation](#6-final-reconciliation)
7. [Important production observations](#7-important-production-observations)

---

## 1. Framework configuration

### 1.1 Storage and table layout

| Entity | Landing path | Checkpoint path | Bronze table |
| --- | --- | --- | --- |
| Orders | `production_session_1/landing/orders/` | `production_session_1/checkpoints/orders/` | `orders_bronze_framework` |
| Customers | `production_session_1/landing/customers/` | `production_session_1/checkpoints/customers/` | `customers_bronze_framework` |
| Products | `production_session_1/landing/products/` | `production_session_1/checkpoints/products/` | `products_bronze_framework` |

All ADLS paths are under:

```text
abfss://data@demodb117.dfs.core.windows.net/autoloader/
```

Each entity needs its own checkpoint. A checkpoint contains the file-processing state of one streaming workload and must not be shared by unrelated entities.

### 1.2 Create the catalog objects

```sql
CREATE SCHEMA IF NOT EXISTS auto_loader_demo.sales_data;

USE CATALOG auto_loader_demo;
USE SCHEMA sales_data;
```

### 1.3 Optional clean start

Run this only when the session paths and tables can be reset safely.

```sql
DROP TABLE IF EXISTS auto_loader_demo.sales_data.orders_bronze_framework;
DROP TABLE IF EXISTS auto_loader_demo.sales_data.customers_bronze_framework;
DROP TABLE IF EXISTS auto_loader_demo.sales_data.products_bronze_framework;
```

```python
dbutils.fs.rm(
    "abfss://data@demodb117.dfs.core.windows.net/"
    "autoloader/production_session_1/landing/orders/",
    True
)
dbutils.fs.rm(
    "abfss://data@demodb117.dfs.core.windows.net/"
    "autoloader/production_session_1/landing/customers/",
    True
)
dbutils.fs.rm(
    "abfss://data@demodb117.dfs.core.windows.net/"
    "autoloader/production_session_1/landing/products/",
    True
)

dbutils.fs.rm(
    "abfss://data@demodb117.dfs.core.windows.net/"
    "autoloader/production_session_1/checkpoints/orders/",
    True
)
dbutils.fs.rm(
    "abfss://data@demodb117.dfs.core.windows.net/"
    "autoloader/production_session_1/checkpoints/customers/",
    True
)
dbutils.fs.rm(
    "abfss://data@demodb117.dfs.core.windows.net/"
    "autoloader/production_session_1/checkpoints/products/",
    True
)
```

Deleting a checkpoint resets the processing history of that stream. This reset is suitable only for an isolated learning environment, not as a routine production recovery method.

### 1.4 Define schemas and entity configurations

The framework uses explicit schemas. Unexpected fields are rescued rather than added automatically to the schemas.

```python
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DoubleType,
    TimestampType
)

orders_schema = StructType([
    StructField("order_id", LongType(), True),
    StructField("customer_id", LongType(), True),
    StructField("order_timestamp", TimestampType(), True),
    StructField("amount", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])

customers_schema = StructType([
    StructField("customer_id", LongType(), True),
    StructField("customer_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("email", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])

products_schema = StructType([
    StructField("product_id", LongType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("_corrupt_record", StringType(), True)
])

entity_configs = {
    "orders": {
        "schema": orders_schema,
        "source_path": (
            "abfss://data@demodb117.dfs.core.windows.net/"
            "autoloader/production_session_1/landing/orders/"
        ),
        "checkpoint_path": (
            "abfss://data@demodb117.dfs.core.windows.net/"
            "autoloader/production_session_1/checkpoints/orders/"
        ),
        "target_table": (
            "auto_loader_demo.sales_data.orders_bronze_framework"
        )
    },
    "customers": {
        "schema": customers_schema,
        "source_path": (
            "abfss://data@demodb117.dfs.core.windows.net/"
            "autoloader/production_session_1/landing/customers/"
        ),
        "checkpoint_path": (
            "abfss://data@demodb117.dfs.core.windows.net/"
            "autoloader/production_session_1/checkpoints/customers/"
        ),
        "target_table": (
            "auto_loader_demo.sales_data.customers_bronze_framework"
        )
    },
    "products": {
        "schema": products_schema,
        "source_path": (
            "abfss://data@demodb117.dfs.core.windows.net/"
            "autoloader/production_session_1/landing/products/"
        ),
        "checkpoint_path": (
            "abfss://data@demodb117.dfs.core.windows.net/"
            "autoloader/production_session_1/checkpoints/products/"
        ),
        "target_table": (
            "auto_loader_demo.sales_data.products_bronze_framework"
        )
    }
}
```

### 1.5 Validate the configuration

The source, checkpoint, and target must be unique for every entity.

```python
def validate_entity_configs(configs):
    required_keys = {
        "schema",
        "source_path",
        "checkpoint_path",
        "target_table"
    }

    for entity_name, config in configs.items():
        missing_keys = required_keys - set(config.keys())
        if missing_keys:
            raise ValueError(
                f"{entity_name} is missing: {sorted(missing_keys)}"
            )

    source_paths = [
        config["source_path"] for config in configs.values()
    ]
    checkpoint_paths = [
        config["checkpoint_path"] for config in configs.values()
    ]
    target_tables = [
        config["target_table"] for config in configs.values()
    ]

    if len(source_paths) != len(set(source_paths)):
        raise ValueError("Each entity must have a unique source path.")

    if len(checkpoint_paths) != len(set(checkpoint_paths)):
        raise ValueError("Each entity must have a unique checkpoint path.")

    if len(target_tables) != len(set(target_tables)):
        raise ValueError("Each entity must have a unique target table.")

    print("Configuration validation passed.")


validate_entity_configs(entity_configs)
```

Create the landing directories before writing the first files:

```python
for config in entity_configs.values():
    dbutils.fs.mkdirs(config["source_path"])
```

The notebook identity needs `READ FILES` and `WRITE FILES` on the Unity Catalog external location covering these paths. The write permission is required because the notebook creates the demonstration files and writes checkpoint state.

### 1.6 Create the file-delivery helpers

The helpers write newline-delimited JSON directly to ADLS. Each line represents one record.

```python
import json


def write_json_records(file_path, records):
    file_content = "\n".join(
        json.dumps(record, separators=(",", ":"))
        for record in records
    )

    dbutils.fs.put(
        file_path,
        file_content + "\n",
        overwrite=False
    )


def write_raw_json_lines(file_path, lines):
    dbutils.fs.put(
        file_path,
        "\n".join(lines) + "\n",
        overwrite=False
    )
```

`overwrite=False` prevents the demonstration from silently replacing an existing delivery. Production landing files should normally be immutable and use unique names.

### 1.7 Define the reusable Auto Loader framework

```python
from pyspark.sql.functions import col, current_timestamp, lit


def build_entity_stream(entity_name, config):
    source_df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .option("readerCaseSensitive", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(config["schema"])
        .load(config["source_path"])
    )

    return (
        source_df
        .withColumn("entity_name", lit(entity_name))
        .withColumn("source_file_path", col("_metadata.file_path"))
        .withColumn("source_file_name", col("_metadata.file_name"))
        .withColumn(
            "source_file_modified_at",
            col("_metadata.file_modification_time")
        )
        .withColumn("ingested_at", current_timestamp())
    )


def run_entity(entity_name, config):
    entity_df = build_entity_stream(entity_name, config)

    query = (
        entity_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", config["checkpoint_path"])
        .trigger(availableNow=True)
        .toTable(config["target_table"])
    )

    query.awaitTermination()
    print(f"Completed: {entity_name}")


def run_all_entities(configs):
    for entity_name, config in configs.items():
        run_entity(entity_name, config)
```

The policy is fixed for this framework:

| Input condition | Result |
| --- | --- |
| Expected field and datatype | Parsed into its defined column |
| Unexpected field | Stored in `_rescued_data` |
| Datatype mismatch | Original field and value stored in `_rescued_data` |
| Case mismatch | Stored in `_rescued_data` when case-sensitive reading is enabled |
| Malformed JSON | Original text stored in `_corrupt_record` |

`cloudFiles.schemaLocation` and write-side `mergeSchema` are not configured. The explicit schemas remain unchanged during these exercises.

---

## 2. Scenario 1: valid multi-entity ingestion

### Cell 1 — Generate three valid files

```python
write_json_records(
    entity_configs["orders"]["source_path"] + "orders_001.json",
    [
        {
            "order_id": 1001,
            "customer_id": 501,
            "order_timestamp": "2026-08-13T09:10:00Z",
            "amount": 2499.00,
            "status": "CONFIRMED"
        },
        {
            "order_id": 1002,
            "customer_id": 502,
            "order_timestamp": "2026-08-13T09:18:00Z",
            "amount": 799.00,
            "status": "SHIPPED"
        },
        {
            "order_id": 1003,
            "customer_id": 503,
            "order_timestamp": "2026-08-13T09:25:00Z",
            "amount": 1499.00,
            "status": "CONFIRMED"
        }
    ]
)

write_json_records(
    entity_configs["customers"]["source_path"] + "customers_001.json",
    [
        {
            "customer_id": 501,
            "customer_name": "Aarav Sharma",
            "city": "Pune",
            "email": "aarav@example.com"
        },
        {
            "customer_id": 502,
            "customer_name": "Priya Deshmukh",
            "city": "Mumbai",
            "email": "priya@example.com"
        },
        {
            "customer_id": 503,
            "customer_name": "Neha Verma",
            "city": "Delhi",
            "email": "neha@example.com"
        }
    ]
)

write_json_records(
    entity_configs["products"]["source_path"] + "products_001.json",
    [
        {
            "product_id": 701,
            "product_name": "Mechanical Keyboard",
            "category": "Accessories",
            "price": 2499.00
        },
        {
            "product_id": 702,
            "product_name": "Wireless Mouse",
            "category": "Accessories",
            "price": 799.00
        },
        {
            "product_id": 703,
            "product_name": "USB-C Hub",
            "category": "Accessories",
            "price": 1499.00
        }
    ]
)
```

### Cell 2 — Process all entities

```python
run_all_entities(entity_configs)
```

### Cell 3 — Validate

```sql
SELECT
    'orders' AS entity,
    COUNT(*) AS total_rows,
    COUNT_IF(_rescued_data IS NOT NULL) AS rescued_rows,
    COUNT_IF(_corrupt_record IS NOT NULL) AS corrupt_rows
FROM auto_loader_demo.sales_data.orders_bronze_framework

UNION ALL

SELECT
    'customers',
    COUNT(*),
    COUNT_IF(_rescued_data IS NOT NULL),
    COUNT_IF(_corrupt_record IS NOT NULL)
FROM auto_loader_demo.sales_data.customers_bronze_framework

UNION ALL

SELECT
    'products',
    COUNT(*),
    COUNT_IF(_rescued_data IS NOT NULL),
    COUNT_IF(_corrupt_record IS NOT NULL)
FROM auto_loader_demo.sales_data.products_bronze_framework;
```

Expected result:

| Entity | Total rows | Rescued rows | Corrupt rows |
| --- | ---: | ---: | ---: |
| Orders | 3 | 0 | 0 |
| Customers | 3 | 0 | 0 |
| Products | 3 | 0 | 0 |

---

## 3. Scenario 2: incremental files and repeated business keys

This delivery contains new files for all three entities. `orders_002.json` repeats `order_id=1002` to distinguish file-level idempotency from business-key deduplication.

### Cell 1 — Generate three incremental files

```python
write_json_records(
    entity_configs["orders"]["source_path"] + "orders_002.json",
    [
        {
            "order_id": 1004,
            "customer_id": 504,
            "order_timestamp": "2026-08-13T10:05:00Z",
            "amount": 1899.00,
            "status": "CONFIRMED"
        },
        {
            "order_id": 1002,
            "customer_id": 502,
            "order_timestamp": "2026-08-13T10:10:00Z",
            "amount": 799.00,
            "status": "SHIPPED"
        }
    ]
)

write_json_records(
    entity_configs["customers"]["source_path"] + "customers_002.json",
    [
        {
            "customer_id": 504,
            "customer_name": "Rohan Patel",
            "city": "Ahmedabad",
            "email": "rohan@example.com"
        },
        {
            "customer_id": 505,
            "customer_name": "Ishita Rao",
            "city": "Bengaluru"
        }
    ]
)

write_json_records(
    entity_configs["products"]["source_path"] + "products_002.json",
    [
        {
            "product_id": 704,
            "product_name": "Webcam",
            "category": "Accessories",
            "price": 1899.00
        },
        {
            "product_id": 705,
            "product_name": "Laptop Stand",
            "category": "Office",
            "price": 1299.00
        }
    ]
)
```

The missing `email` field for customer `505` becomes `NULL`. It is not corrupt and is not rescued because the field already exists in the declared schema.

### Cell 2 — Process all entities

```python
run_all_entities(entity_configs)
```

### Cell 3 — Validate incremental behaviour

```sql
SELECT
    source_file_name,
    COUNT(*) AS rows_from_file
FROM auto_loader_demo.sales_data.orders_bronze_framework
GROUP BY source_file_name
ORDER BY source_file_name;
```

```sql
SELECT
    order_id,
    COUNT(*) AS occurrences,
    COLLECT_SET(source_file_name) AS source_files
FROM auto_loader_demo.sales_data.orders_bronze_framework
GROUP BY order_id
HAVING COUNT(*) > 1;
```

Expected observation:

- Only the three `_002.json` files contribute new rows during this run.
- `order_id=1002` occurs twice because it arrived in two different files.
- Auto Loader tracks files; it does not enforce uniqueness of business keys.
- Business-key deduplication requires a separate transformation or merge policy.

Run the framework once more without creating files:

```python
run_all_entities(entity_configs)
```

The row counts remain unchanged because the same checkpoints remember the files already processed.

---

## 4. Scenario 3: corrupt JSON records

A corrupt record cannot be parsed as a complete JSON object. In `PERMISSIVE` mode, its original text is stored in `_corrupt_record`, while other parseable records remain available.

### Cell 1 — Generate three files containing different conditions

```python
write_raw_json_lines(
    entity_configs["orders"]["source_path"] + "orders_003.json",
    [
        '{"order_id":1005,"customer_id":505,"order_timestamp":"2026-08-13T11:00:00Z","amount":3299.0,"status":"CONFIRMED"}',
        '{"order_id":1006,"customer_id":506,"order_timestamp":"2026-08-13T11:05:00Z","amount":not-a-number,"status":"SHIPPED"}',
        '{"order_id":1007,"customer_id":507,"order_timestamp":"2026-08-13T11:10:00Z","amount":499.0,"status":"CONFIRMED"}'
    ]
)

write_raw_json_lines(
    entity_configs["customers"]["source_path"] + "customers_003.json",
    [
        '{"customer_id":506,"customer_name":"Kabir Singh","city":"Jaipur","email":"kabir@example.com"}',
        'this is not a JSON record',
        '{"customer_id":507,"customer_name":"Meera Iyer","city":"Chennai","email":"meera@example.com"}'
    ]
)

write_raw_json_lines(
    entity_configs["products"]["source_path"] + "products_003.json",
    [
        '{"product_id":706,"product_name":"Headset","category":"Accessories","price":3299.0}',
        '{"product_id":707,"product_name":"Desk Mat","category":"Office","price":999.0}',
        '{"product_id":708,"product_name":"Cable Organizer","category":"Office","price":499.0}'
    ]
)
```

### Cell 2 — Process all entities

```python
run_all_entities(entity_configs)
```

### Cell 3 — Validate corrupt records

```sql
SELECT
    entity_name,
    source_file_name,
    _corrupt_record,
    ingested_at
FROM auto_loader_demo.sales_data.orders_bronze_framework
WHERE _corrupt_record IS NOT NULL

UNION ALL

SELECT
    entity_name,
    source_file_name,
    _corrupt_record,
    ingested_at
FROM auto_loader_demo.sales_data.customers_bronze_framework
WHERE _corrupt_record IS NOT NULL

UNION ALL

SELECT
    entity_name,
    source_file_name,
    _corrupt_record,
    ingested_at
FROM auto_loader_demo.sales_data.products_bronze_framework
WHERE _corrupt_record IS NOT NULL;
```

Expected observation:

- One order line and one customer line are corrupt.
- Valid lines from the same files are still parsed.
- The valid products file is processed normally.
- A problem in one entity does not change another entity's checkpoint or schema.

Malformed syntax and a missing field are different conditions:

| Condition | Result |
| --- | --- |
| Missing optional field | Expected column contains `NULL` |
| Unexpected field | Field is stored in `_rescued_data` |
| Wrong datatype | Field is stored in `_rescued_data` |
| Malformed JSON | Original record is stored in `_corrupt_record` |

---

## 5. Scenario 4: rescued data

Rescued data is structurally valid JSON that does not match the declared schema. The original unexpected field names and values are preserved in `_rescued_data`.

### Cell 1 — Generate three schema-mismatch files

```python
write_json_records(
    entity_configs["orders"]["source_path"] + "orders_004.json",
    [
        {
            "order_id": 1008,
            "customer_id": 508,
            "order_timestamp": "2026-08-13T12:00:00Z",
            "amount": 2799.00,
            "status": "CONFIRMED",
            "discount_code": "WELCOME10"
        },
        {
            "order_id": 1009,
            "customer_id": 509,
            "order_timestamp": "2026-08-13T12:05:00Z",
            "amount": 1599.00,
            "status": "PROCESSING"
        }
    ]
)

write_json_records(
    entity_configs["customers"]["source_path"] + "customers_004.json",
    [
        {
            "customer_id": "C-508",
            "customer_name": "Ananya Gupta",
            "city": "Hyderabad",
            "email": "ananya@example.com"
        },
        {
            "customer_id": 509,
            "customer_name": "Vikram Joshi",
            "city": "Nagpur",
            "email": "vikram@example.com"
        }
    ]
)

write_json_records(
    entity_configs["products"]["source_path"] + "products_004.json",
    [
        {
            "Product_ID": 709,
            "product_name": "Portable SSD",
            "category": "Storage",
            "price": 6999.00
        },
        {
            "product_id": 710,
            "product_name": "USB Cable",
            "category": "Accessories",
            "price": "price-unavailable"
        }
    ]
)
```

This delivery contains four different schema outcomes:

| Record | Condition |
| --- | --- |
| Order `1008` | Unexpected `discount_code` field |
| Customer `C-508` | String supplied for a `LONG` field |
| Product `709` | `Product_ID` does not match `product_id` case |
| Product `710` | Invalid string supplied for a `DOUBLE` field |

### Cell 2 — Process all entities

```python
run_all_entities(entity_configs)
```

### Cell 3 — Validate rescued data

```sql
SELECT
    entity_name,
    source_file_name,
    order_id AS business_id,
    _rescued_data
FROM auto_loader_demo.sales_data.orders_bronze_framework
WHERE _rescued_data IS NOT NULL

UNION ALL

SELECT
    entity_name,
    source_file_name,
    customer_id AS business_id,
    _rescued_data
FROM auto_loader_demo.sales_data.customers_bronze_framework
WHERE _rescued_data IS NOT NULL

UNION ALL

SELECT
    entity_name,
    source_file_name,
    product_id AS business_id,
    _rescued_data
FROM auto_loader_demo.sales_data.products_bronze_framework
WHERE _rescued_data IS NOT NULL;
```

For a datatype or identifier mismatch, the parsed business column can be `NULL` while the original field and value remain inside `_rescued_data`.

Inspect the rescued JSON directly:

```sql
SELECT
    order_id,
    get_json_object(
        _rescued_data,
        '$.discount_code'
    ) AS rescued_discount_code,
    source_file_name
FROM auto_loader_demo.sales_data.orders_bronze_framework
WHERE _rescued_data IS NOT NULL;
```

The schema has not evolved. `discount_code` is not a new Delta table column; it remains inside `_rescued_data` for investigation and controlled handling.

---

## 6. Final reconciliation

### Compare all three entity results

```sql
SELECT
    'orders' AS entity,
    COUNT(DISTINCT source_file_name) AS processed_files,
    COUNT(*) AS bronze_rows,
    COUNT_IF(_rescued_data IS NOT NULL) AS rescued_rows,
    COUNT_IF(_corrupt_record IS NOT NULL) AS corrupt_rows
FROM auto_loader_demo.sales_data.orders_bronze_framework

UNION ALL

SELECT
    'customers',
    COUNT(DISTINCT source_file_name),
    COUNT(*),
    COUNT_IF(_rescued_data IS NOT NULL),
    COUNT_IF(_corrupt_record IS NOT NULL)
FROM auto_loader_demo.sales_data.customers_bronze_framework

UNION ALL

SELECT
    'products',
    COUNT(DISTINCT source_file_name),
    COUNT(*),
    COUNT_IF(_rescued_data IS NOT NULL),
    COUNT_IF(_corrupt_record IS NOT NULL)
FROM auto_loader_demo.sales_data.products_bronze_framework;
```

Expected result after all four scenarios:

| Entity | Processed files | Bronze rows | Rescued rows | Corrupt rows |
| --- | ---: | ---: | ---: | ---: |
| Orders | 4 | 10 | 1 | 1 |
| Customers | 4 | 10 | 1 | 1 |
| Products | 4 | 10 | 2 | 0 |

The corrupt rows remain in the Bronze count because `PERMISSIVE` mode preserves them for investigation.

### Inspect the delivered files

```python
for entity_name, config in entity_configs.items():
    print(f"\n{entity_name.upper()}")
    for file_info in dbutils.fs.ls(config["source_path"]):
        print(file_info.name)
```

### Run the no-new-file test

```python
run_all_entities(entity_configs)
```

Run the reconciliation query again. The counts must remain unchanged.

---

## 7. Important production observations

1. **File idempotency is not business-key deduplication.** Auto Loader avoids processing the same discovered file again with the same checkpoint. It does not prevent the same `order_id` from arriving in another file.

2. **Every entity requires independent state.** Source paths, checkpoints, schemas, and target tables should not be shared across unrelated entities.

3. **A missing field is not automatically bad data.** If the field exists in the declared schema, a missing value normally becomes `NULL`. Business rules decide whether that `NULL` is acceptable.

4. **Corrupt and rescued records represent different failures.** `_corrupt_record` contains input that cannot be parsed as JSON. `_rescued_data` contains parseable JSON fields that do not match the expected schema.

5. **Rescued data requires monitoring.** A pipeline can continue successfully while rescued records accumulate. A successful job run does not guarantee that every business field was parsed correctly.

6. **The schemas are intentionally fixed.** Automatic schema inference, `cloudFiles.schemaLocation`, `addNewColumns`, restart after `UnknownFieldException`, and Delta `mergeSchema` are separate schema-evolution concerns.

7. **Landing files should be immutable.** Use unique filenames for new deliveries instead of overwriting files that Auto Loader might already have processed.

8. **`AvailableNow` is one incremental run.** It processes files available when the query starts and then terminates. A later delivery requires another job run.

## References

- [Configure Auto Loader schema inference and evolution](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/schema)
- [Configure Auto Loader for production workloads](https://learn.microsoft.com/en-us/azure/databricks/ingestion/cloud-object-storage/auto-loader/production)
- [Databricks Utilities reference](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-utils)
- [Work with files on Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/files/)
