# Session 2: Productionizing File-Based Structured Streaming

This project continues the same local file-to-raw-zone prototype. It does not introduce Bronze, Silver, joins, deduplication, Delta Lake, or business transformations.

## Session objective

Make the existing raw-zone ingestion pipeline more production-ready by covering:

1. Multiple arriving files
2. Input throttling
3. Checkpoint-based recovery
4. Replay after checkpoint deletion
5. Malformed-input handling with `FAILFAST`
6. Malformed-input handling with `PERMISSIVE`
7. Basic streaming monitoring

## Project structure

```text
pyspark_session2_raw_zone_exercises/
├── sample_files/
│   ├── orders_001.csv
│   ├── orders_002.csv
│   ├── orders_003.csv
│   ├── orders_004.csv
│   └── orders_bad.csv
├── runtime/
├── ex1_multiple_file_ingestion.py
├── ex2_max_files_per_trigger.py
├── ex3_checkpoint_restart_recovery.py
├── ex4_checkpoint_deletion_replay.py
├── ex5_bad_records_failfast.py
├── ex6_bad_records_permissive.py
├── ex7_streaming_query_monitoring.py
├── inspect_raw_zone.py
└── requirements.txt
```

Every exercise creates its own local folders under `runtime/exN/`:

```text
runtime/exN/
├── landing/orders/incoming/
├── raw_zone/orders/
└── checkpoints/orders/
```

## Setup

```powershell
cd pyspark_session2_raw_zone_exercises

python -m venv .venv
.\.venv\Scripts\Activate.ps1

pip install -r requirements.txt
```

You also need a compatible local Java installation available through `JAVA_HOME`.

---

## Exercise 1: Multiple-file ingestion

Start the job:

```powershell
python .\ex1_multiple_file_ingestion.py --reset
```

In a second PowerShell window:

```powershell
Copy-Item .\sample_files\orders_001.csv .\runtime\ex1\landing\orders\incoming\
Copy-Item .\sample_files\orders_002.csv .\runtime\ex1\landing\orders\incoming\
```

Later, add another file:

```powershell
Copy-Item .\sample_files\orders_003.csv .\runtime\ex1\landing\orders\incoming\
```

Inspect the output:

```powershell
python .\inspect_raw_zone.py --exercise ex1
```

---

## Exercise 2: `maxFilesPerTrigger`

Start the job:

```powershell
python .\ex2_max_files_per_trigger.py --reset
```

Copy three files together:

```powershell
Copy-Item .\sample_files\orders_001.csv .\runtime\ex2\landing\orders\incoming\
Copy-Item .\sample_files\orders_002.csv .\runtime\ex2\landing\orders\incoming\
Copy-Item .\sample_files\orders_003.csv .\runtime\ex2\landing\orders\incoming\
```

The script is configured with:

```python
.option("maxFilesPerTrigger", 1)
```

Spark should consume at most one new file in each micro-batch.

---

## Exercise 3: Checkpoint restart recovery

First run:

```powershell
python .\ex3_checkpoint_restart_recovery.py --reset
```

Copy the first file:

```powershell
Copy-Item .\sample_files\orders_001.csv .\runtime\ex3\landing\orders\incoming\
```

After it is processed, stop the job with `Ctrl+C`. While the job is stopped:

```powershell
Copy-Item .\sample_files\orders_002.csv .\runtime\ex3\landing\orders\incoming\
```

Restart without resetting:

```powershell
python .\ex3_checkpoint_restart_recovery.py
```

Expected result: Spark processes the second file and does not process the first file again.

---

## Exercise 4: Replay after checkpoint deletion

Start and process one file:

```powershell
python .\ex4_checkpoint_deletion_replay.py --reset
```

```powershell
Copy-Item .\sample_files\orders_001.csv .\runtime\ex4\landing\orders\incoming\
```

Stop the job. Delete only its checkpoint:

```powershell
Remove-Item .\runtime\ex4\checkpoints\orders -Recurse -Force
```

Restart without `--reset`:

```powershell
python .\ex4_checkpoint_deletion_replay.py
```

The source file is still present, but Spark has lost its progress history. The file can be ingested again, creating duplicate raw records.

Inspect:

```powershell
python .\inspect_raw_zone.py --exercise ex4
```

---

## Exercise 5: Bad input with `FAILFAST`

Start:

```powershell
python .\ex5_bad_records_failfast.py --reset
```

Copy the malformed file:

```powershell
Copy-Item .\sample_files\orders_bad.csv .\runtime\ex5\landing\orders\incoming\
```

Expected result: the micro-batch fails because `quantity` contains `two` instead of an integer.

---

## Exercise 6: Bad input with `PERMISSIVE`

Start:

```powershell
python .\ex6_bad_records_permissive.py --reset
```

Copy the malformed file:

```powershell
Copy-Item .\sample_files\orders_bad.csv .\runtime\ex6\landing\orders\incoming\
```

Inspect:

```powershell
python .\inspect_raw_zone.py --exercise ex6
```

Look at the `_corrupt_record` column. This demonstrates retaining evidence of malformed input while allowing the stream to continue.

---

## Exercise 7: Streaming query monitoring

Start:

```powershell
python .\ex7_streaming_query_monitoring.py --reset
```

Copy files one by one:

```powershell
Copy-Item .\sample_files\orders_001.csv .\runtime\ex7\landing\orders\incoming\
Copy-Item .\sample_files\orders_002.csv .\runtime\ex7\landing\orders\incoming\
```

The script prints:

- Query status
- Batch ID
- Number of input rows
- Input rows per second
- Processed rows per second
- Trigger duration

## Recommended teaching order

```text
Ex1 Multiple files
  ↓
Ex2 Input throttling
  ↓
Ex3 Checkpoint recovery
  ↓
Ex4 Checkpoint deletion and replay
  ↓
Ex5 FAILFAST
  ↓
Ex6 PERMISSIVE
  ↓
Ex7 Monitoring
```

## Atomic file-arrival recommendation

A producer should finish writing a file before placing it into the monitored landing folder. For a local demonstration, write or copy to a temporary location first and then move it:

```powershell
Copy-Item .\sample_files\orders_001.csv .\orders_001.tmp
Move-Item .\orders_001.tmp .\runtime\ex1\landing\orders\incoming\orders_001.csv
```

The move makes the completed file visible to Spark at once.
