# PySpark Shell Command Sheet

## Load the helpers

```python
exec(open("customer_schema_evolution_shell_helpers.py", encoding="utf-8").read())
list_demo_pairs()
```

## Baseline

```python
scenario = "baseline"
prepare_scenario(scenario)
explain_scenario(scenario)
show_input_file(scenario)
q = start_customer_stream(scenario)
arrive_file(scenario)
process_available_data_safely(scenario)
show_raw_output(scenario)
stop_customer_stream(scenario)
```

## Added column

```python
show_pair_commands("added_column")
```

Problem: `added_column_old_schema`  
Solution: `added_column_new_schema`

## Old file after upgrade

```python
show_pair_commands("old_file_after_upgrade")
```

Problem: `old_file_new_schema`  
Solution: `old_file_new_schema_handled`

## Reordered columns

```python
show_pair_commands("reordered_columns")
```

Problem: `reordered_columns_unsafe`  
Solution: `reordered_columns_mapped`

## Renamed columns

```python
show_pair_commands("renamed_columns")
```

Problem: `renamed_columns_rejected`  
Solution: `renamed_columns_mapped`

The problem query is expected to fail. Inspect it with:

```python
show_query_status("renamed_columns_rejected")
stop_customer_stream("renamed_columns_rejected")
```

## Datatype change

```python
show_pair_commands("datatype_change")
```

Problem: `datatype_change_old_schema`  
Solution: `datatype_change_new_schema`

## Useful commands

```python
list_scenarios()
explain_scenario("added_column_new_schema")
show_input_file("added_column_new_schema")
show_query_status("added_column_new_schema")
stop_all_customer_streams()
```
