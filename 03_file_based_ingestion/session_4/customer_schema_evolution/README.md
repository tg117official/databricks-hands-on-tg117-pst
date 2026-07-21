# Customer Schema Evolution with the PySpark Shell

This package demonstrates schema-evolution problems and their practical solutions using one customer file stream.

Nothing runs automatically. Every step is executed from the PySpark shell so the input, query behaviour, output, and fix can be shown separately.

## Package contents

```text
customer_schema_evolution_pyspark_shell/
├── README.md
├── customer_schema_evolution_notes.md
├── customer_schema_evolution_shell_helpers.py
├── shell_demo_commands.md
└── sample_files/
    ├── customers_01_baseline.csv
    ├── customers_02_added_phone_old_schema.csv
    ├── customers_03_old_file_new_schema.csv
    ├── customers_04_reordered_columns.csv
    ├── customers_05_renamed_columns.csv
    └── customers_06_alphanumeric_id.csv
```

Runtime folders are created while the exercises run:

```text
schema_evolution_runtime/
├── landing/customers/<scenario>/incoming/
├── raw_zone/customers/<scenario>/batches/
└── checkpoints/customers/<scenario>/
```

## Start

Open PowerShell in this project folder:

```powershell
pyspark
```

Load the helper file:

```python
exec(
    open(
        "customer_schema_evolution_shell_helpers.py",
        encoding="utf-8",
    ).read()
)
```

List the problem–solution pairs:

```python
list_demo_pairs()
```

Print the commands for one pair:

```python
show_pair_commands("added_column")
```

## Problem–solution pairs

| Pair | Problem | Practical solution |
|---|---|---|
| `added_column` | V2 file is read using V1 | Upgrade to schema V2 |
| `old_file_after_upgrade` | Old file produces a missing V2 field | Apply a temporary agreed default |
| `reordered_columns` | Positional mapping swaps values | Match source order and select canonical order |
| `renamed_columns` | Header validation rejects renamed fields | Alias new source names to canonical names |
| `datatype_change` | Alphanumeric ID becomes null under IntegerType | Use StringType and validate the key |

## Standard shell flow

```python
scenario = "added_column_old_schema"

prepare_scenario(scenario)
explain_scenario(scenario)
show_input_file(scenario)

q = start_customer_stream(scenario)
arrive_file(scenario)
process_available_data_safely(scenario)

show_raw_output(scenario)
stop_customer_stream(scenario)
```

Run the matching solution in the same way, then compare:

```python
compare_pair_outputs("added_column")
```

## Output format

Each successful micro-batch is written as a CSV file with a header:

```text
schema_evolution_runtime/raw_zone/customers/<scenario>/batches/
└── batch_00000000000000000000/
    └── customers_batch_00000000000000000000.csv
```

`coalesce(1)` is used only to make the small local result easy to open. Large production batches should normally keep parallel output files.

## Expected-failure scenario

`renamed_columns_rejected` intentionally fails header validation.

After `process_available_data_safely()` reports the error, inspect and stop the query:

```python
show_query_status("renamed_columns_rejected")
stop_customer_stream("renamed_columns_rejected")
```

Then run `renamed_columns_mapped` to demonstrate the practical fix.
