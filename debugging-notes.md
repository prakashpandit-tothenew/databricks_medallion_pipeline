# Debugging Notes

These notes record failures that actually occurred while running this repository, not hypothetical Spark errors.

## 1. GitHub push 403

**Symptom:** `Permission to prakashpandit-tothenew/databricks_medallion_pipeline.git denied to Ppandit-MAF`.

**Cause:** Git Credential Manager used a different GitHub login than the remote owner. `user.name` was unrelated.

**Resolution:** Erase stored `github.com` credentials and sign in as the account that owns the remote. Do not change `origin` unless the repo should live under another user.

## 2. Databricks notebook argparse

**Symptom:** `db_ipykernel_launcher.py: error: unrecognized arguments: -f /local_disk0/.../connection.json`.

**Cause:** Executing `main()` / `%run` of a `.py` file lets the kernel inject `-f`.

**Resolution:** `parser.parse_known_args()` in Bronze, Silver, and Gold entrypoints. Prefer calling `ingest_*` / `transform_all` / `create_gold_tables` with the notebook `spark` object.

## 3. Bronze source check (Spark Connect)

**Symptom:** `[BRONZE FAILED] ... duration_seconds=0.00` with no useful error at first.

**Cause:** `source_exists` used `spark.sparkContext._jvm`, which Spark Connect / serverless does not expose.

**Resolution:** Existence check via `spark.read.text(path).limit(1)`. Log `error={type}: {message}` on failure.

## 4. Unity Catalog `input_file_name`

**Symptom:** `UC_COMMAND_NOT_SUPPORTED ... input_file_name ... use _metadata.file_path`.

**Cause:** CSV was read (10,000 rows); metadata used a Hive-era function.

**Resolution:** `_source_file = col("_metadata.file_path")`. Sync `_common.py` and `%restart_python` so Databricks is not running a cached module.

## 5. Missing cloud file system scheme

**Symptom:** `INVALID_PARAMETER_VALUE ... Missing cloud file system scheme` during Delta write / `CREATE TABLE ... LOCATION`.

**Cause:** Unity Catalog would not treat `dbfs:/tmp/...` or `/Volumes/...` as an external table location with `saveAsTable` + `path` / `LOCATION`.

**Resolution:** Managed tables `poc_catalog.default.bronze_*` (and later Silver/Gold) with `saveAsTable` / `CREATE OR REPLACE TABLE` and **no** `LOCATION` clause. CSVs remain on the Volume; table files are catalog-managed.

## 6. Gold `__file__` missing

**Symptom:** `NameError: name '__file__' is not defined` at Gold start.

**Cause:** Notebook execution has no script path.

**Resolution:** `create_gold_tables(spark, sql_dir="/Workspace/Users/.../src/gold")` and a directory search fallback.

## 7. Gold tables all zero rows

**Symptom:** Four Gold tables complete with `rows=0`. Later: `No Silver customers have quality_check_result PASSED/PASS`.

**Cause:** Gold filters `PASSED`. If Bronze was ingested more than once, Silver uniqueness flags every duplicated `customer_id`, so almost no customer is `PASSED`. Empty or untransformed Silver produces the same Gold emptiness.

**Resolution:** Preflight prints Silver totals and distinct `quality_check_result` values. Drop Bronze/Silver/Gold development tables, ingest Bronze **once**, then Silver, then Gold.

## 8. Local PySpark vs Databricks

**Symptom:** Local `pyspark` install, `winutils` warnings, worker socket resets, no Delta/UC.

**Cause:** Assessment runtime is Databricks, not Windows local Spark.

**Resolution:** Use local Spark only for CSV/DQ unit checks; persist Delta on Databricks.
