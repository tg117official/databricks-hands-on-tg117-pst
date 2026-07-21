# Customer Schema Evolution with the PySpark Shell

This package is designed for an interactive PySpark shell demonstration using one customer file stream. Nothing runs automatically. Each scenario can be prepared, started, observed, and stopped independently.

## Project structure

```text
customer_schema_evolution_pyspark_shell/
├── customer_schema_evolution_shell_helpers.py
├── customer_schema_evolution_notes.md
├── sample_files/
│   ├── customers_01_baseline.csv
│   ├── customers_02_added_phone_old_schema.csv
│   ├── customers_03_old_file_new_schema.csv
│   ├── customers_04_reordered_columns.csv
│   ├── customers_05_renamed_columns.csv
│   └── customers_06_alphanumeric_id.csv
└── schema_evolution_runtime/       # Created while running the exercises
```

## Start the shell

Open PowerShell in this project directory:

```powershell
pyspark
```

Load the reusable definitions:

```python
exec(
    open(
        "customer_schema_evolution_shell_helpers.py",
        encoding="utf-8",
    ).read()
)
```

List the available scenarios:

```python
list_scenarios()
```

## Raw-zone output format

Each successful micro-batch is written as a CSV file with a header:

```text
schema_evolution_runtime/raw_zone/customers/<scenario>/batches/
└── batch_00000000000000000000/
    └── customers_batch_00000000000000000000.csv
```

`coalesce(1)` is used only to make this small local output easy to open. Large production batches should normally retain parallel output files.

## Standard shell flow

```python
scenario = "baseline"

prepare_scenario(scenario)
explain_scenario(scenario)
show_input_file(scenario)

q = start_customer_stream(scenario)
arrive_file(scenario)
process_available_data(scenario)

show_raw_output(scenario)
stop_customer_stream(scenario)
```

For every non-baseline scenario, `explain_scenario()` now displays:

```text
Problem
What Spark does
Why it matters
Possible solutions
Recommended approach
```

## Scenario summary

| Scenario | Problem demonstrated | Recommended direction |
|---|---|---|
| `baseline` | File and schema match | Use as the reference result |
| `added_column_old_schema` | V2 file arrives while query still uses V1 | Upgrade to nullable V2 and version the deployment |
| `old_file_new_schema` | V1 file arrives after V2 deployment | Allow null temporarily, monitor, then enforce cutover |
| `reordered_columns_unsafe` | Positional mapping swaps values silently | Reject unexpected order and validate headers |
| `reordered_columns_safe` | Header validation stops the bad file | Keep the safeguard; correct or quarantine the file |
| `renamed_columns` | New names break the contract | Map explicitly or release a new version |
| `datatype_change_old_schema` | Alphanumeric ID becomes null under integer schema | Reject invalid key and move to string-based V3 |
| `datatype_change_new_schema` | Raw ingestion works but consumers may expect integer | Make string canonical and migrate downstream systems |

## Run a successful scenario

```python
scenario = "added_column_old_schema"
prepare_scenario(scenario)
explain_scenario(scenario)
show_input_file(scenario)
q = start_customer_stream(scenario)
arrive_file(scenario)
process_available_data(scenario)
show_raw_output(scenario)
stop_customer_stream(scenario)
```

## Run an expected-failure scenario

```python
scenario = "reordered_columns_safe"
prepare_scenario(scenario)
explain_scenario(scenario)
show_input_file(scenario)
q = start_customer_stream(scenario)
arrive_file(scenario)
process_available_data(scenario)
```

Inspect and stop the failed query:

```python
show_query_status(scenario)
stop_customer_stream(scenario)
```

The same pattern applies to:

```python
scenario = "renamed_columns"
```

## Useful shell commands

```python
show_query_status(scenario)
q = get_query(scenario)
q.status
q.lastProgress
q.exception()
stop_all_customer_streams()
exit()
```

## Recommended session order

```text
Baseline
  ↓
New column with old schema: problem and upgrade options
  ↓
Old file with new schema: compatibility window
  ↓
Reordered columns: silent corruption and safe rejection
  ↓
Renamed columns: explicit mapping or versioning
  ↓
Datatype change: failed old contract and controlled V3 migration
```

The main production lesson is:

> A schema mismatch is not only an error to observe. It is a compatibility problem that needs an explicit response: accept temporarily, reject, map, version, or migrate.
