# Customer Schema Evolution with the PySpark Shell

This version is designed for an interactive demonstration. The helper file only
defines schemas, paths, scenario configuration, and reusable functions. It does
not start or run a streaming query automatically.

## Project structure

```text
customer_schema_evolution_pyspark_shell/
├── customer_schema_evolution_shell_helpers.py
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

See the available scenarios:

```python
list_scenarios()
```

## Standard demonstration pattern

Use the same six commands for every scenario:

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

Starting the stream before `arrive_file()` makes the file-arrival behaviour
visible. `process_available_data()` removes the need to wait for an uncertain
number of trigger intervals during a live demonstration.

---

## Exercise 1: Baseline schema

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

Expected result: all V1 columns are stored correctly.

---

## Exercise 2: New source column with the old Spark schema

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

Expected observation: the pipeline still exposes schema V1 and does not retain
`phone_number`. An explicit Spark schema does not evolve automatically just
because a source file contains another column.

---

## Exercise 3: Old file after upgrading to schema V2

```python
scenario = "old_file_new_schema"
prepare_scenario(scenario)
explain_scenario(scenario)
show_input_file(scenario)
q = start_customer_stream(scenario)
arrive_file(scenario)
process_available_data(scenario)
show_raw_output(scenario)
stop_customer_stream(scenario)
```

Expected observation: `phone_number` is null. This is a common backward-
compatibility pattern for a newly added optional field.

---

## Exercise 4A: Reordered columns with positional mapping

```python
scenario = "reordered_columns_unsafe"
prepare_scenario(scenario)
explain_scenario(scenario)
show_input_file(scenario)
q = start_customer_stream(scenario)
arrive_file(scenario)
process_available_data(scenario)
show_raw_output(scenario)
stop_customer_stream(scenario)
```

Expected observation: the job can succeed while `email` and `city` contain each
other's values. This is silent data corruption.

## Exercise 4B: Reordered columns with header validation

```python
scenario = "reordered_columns_safe"
prepare_scenario(scenario)
explain_scenario(scenario)
show_input_file(scenario)
q = start_customer_stream(scenario)
arrive_file(scenario)
process_available_data(scenario)
```

The last command is expected to report a header mismatch. After observing it:

```python
show_query_status(scenario)
stop_customer_stream(scenario)
```

---

## Exercise 5: Renamed columns

```python
scenario = "renamed_columns"
prepare_scenario(scenario)
explain_scenario(scenario)
show_input_file(scenario)
q = start_customer_stream(scenario)
arrive_file(scenario)
process_available_data(scenario)
```

The file is expected to fail header validation because `full_name` and
`email_address` do not match the agreed V2 column names.

```python
show_query_status(scenario)
stop_customer_stream(scenario)
```

---

## Exercise 6A: Datatype change using the old schema

```python
scenario = "datatype_change_old_schema"
prepare_scenario(scenario)
explain_scenario(scenario)
show_input_file(scenario)
q = start_customer_stream(scenario)
arrive_file(scenario)
process_available_data(scenario)
show_raw_output(scenario)
stop_customer_stream(scenario)
```

Expected observation: `C110` cannot be represented as an integer and can become
null under permissive parsing. A successful pipeline run does not guarantee
that a business key remains valid.

## Exercise 6B: Controlled upgrade to schema V3

```python
scenario = "datatype_change_new_schema"
prepare_scenario(scenario)
explain_scenario(scenario)
show_input_file(scenario)
q = start_customer_stream(scenario)
arrive_file(scenario)
process_available_data(scenario)
show_raw_output(scenario)
stop_customer_stream(scenario)
```

Expected observation: `C110` is retained because schema V3 stores `customer_id`
as a string.

---

## Useful shell commands

Inspect the query without stopping it:

```python
show_query_status(scenario)
```

Access the query directly:

```python
q = get_query(scenario)
q.status
q.lastProgress
q.exception()
```

Stop all queries before leaving the shell:

```python
stop_all_customer_streams()
```

Exit:

```python
exit()
```

## Recommended session order

```text
Baseline
  ↓
Added optional column
  ↓
Old file with the new schema
  ↓
Reordered columns: unsafe and safe
  ↓
Renamed columns
  ↓
Datatype change: old and upgraded schema
```

The main production lesson is that schema evolution must be classified before
it is accepted:

```text
Add nullable field at the end
    → Usually easier to support

Old file missing the new trailing field
    → Can remain compatible using null

Reorder or rename CSV columns
    → Breaking and potentially dangerous

Change a business-key datatype
    → Breaking; requires controlled versioning and impact analysis
```
