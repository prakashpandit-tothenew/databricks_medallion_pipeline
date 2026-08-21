# AI-Assisted Development: Bronze Layer

## Objective

Implement Phase 3 Bronze ingestion for the generated Customers, Products, and
Orders CSV files using explicit PySpark schemas, raw-row preservation,
append-only Delta tables, lineage metadata, basic ingestion validation, and
simple orchestration. Silver, Gold, Dashboard, and final documentation remain
out of scope.

## Prompt used

The user instructed Cursor to review the project rules, requirements, design,
data-quality strategy, and completed data-generation phase before implementing:

- `src/bronze/01_ingest_customers.py`
- `src/bronze/02_ingest_orders.py`
- `src/bronze/03_ingest_products.py`
- `src/bronze/ingest_all.py`

The prompt required explicit schemas, header-aware CSV reads, preservation of
NULL/duplicate/orphan records, `_ingestion_timestamp` and `_source_file`
metadata, DBFS-compatible Delta tables named `bronze_customers`,
`bronze_products`, and `bronze_orders`, append-only writes, counts/timing logs,
source/table validation, Databricks Community Edition compatibility, and no
downstream-layer logic.

## Implementation approach

1. Reused the exact typed contracts from data generation; data-generation code
   required no integration change.
2. Kept each dataset schema and `ingest_*` function in its numbered entrypoint.
3. Added `_common.py` for technical behavior shared by all three datasets:
   path checks, explicit-schema reads, count/column checks, metadata, append
   writes, target validation, and logs.
4. Used path-backed Hive-metastore Delta tables under
   `dbfs:/tmp/databricks_medallion_pipeline/bronze`.
5. Validated each append as
   `target_after = target_before + source_count`.
6. Used one Spark session in `ingest_all.py`, executing Customers, Products,
   then Orders.

## Cursor suggestions accepted

- Explicit `StructType` / `StructField` contracts with no schema inference.
- `PERMISSIVE` CSV parsing with headers and empty CSV values retained as NULL.
- `current_timestamp()` and `input_file_name()` for Bronze lineage.
- `format("delta").mode("append").option("path", ...).saveAsTable(...)`.
- Top-level Hive-metastore table names instead of Unity Catalog names.
- Hadoop filesystem checks so local, DBFS, and compatible paths work.
- Shared technical utilities to avoid repeating write and validation behavior.
- Dynamic module loading in orchestration because Python import statements
  cannot directly name modules beginning with digits.

## Suggestions rejected

- Overwrite, truncate, or table-drop behavior was rejected because Bronze is
  append-only.
- Deduplication, NULL repair, orphan correction, quality flags, and business
  transformations were rejected as Silver concerns.
- pandas, external orchestrators, cloud services, and credentials were not
  introduced.
- Adding the older `_ingested_at` metadata alias was rejected because the
  current phase explicitly requires `_ingestion_timestamp`; adding both would
  introduce an unrequested column.
- Installing a local Delta extension solely for development verification was
  rejected as unnecessary; Delta is supplied by the target Databricks runtime.

## Manual changes

No manual user edits were reported during this phase. Cursor created the Bronze
modules and this development record.

## Validation performed

- All Bronze Python modules compiled successfully.
- Cursor diagnostics reported no linter errors.
- A prohibited-pattern scan found no schema inference, pandas, overwrite,
  deduplication, Silver quality codes, or row-dropping operations.
- Local PySpark explicit-schema reads passed:
  - Customers: 10,000 rows, schema PASS, metadata PASS.
  - Products: 500 rows, schema PASS, metadata PASS.
  - Orders: 100,000 rows, schema PASS, metadata PASS.
- Preservation checks confirmed 50 NULL customer emails, 10 duplicate customer
  IDs, 100 NULL order customer IDs, 200 NULL order product IDs, and 20
  duplicate order IDs remained after Bronze metadata was added.
- The Delta append/table-count path is implemented but was not executed locally
  because the plain local PySpark runtime does not include Delta Lake. It must
  be run on Databricks Community Edition for table materialization.

## Issues encountered

1. Existing rules/design notes call the timestamp `_ingested_at`, while the
   current Phase 3 prompt requires `_ingestion_timestamp`. The current prompt
   was treated as authoritative.
2. Earlier quality documentation expects approximately 700 defects, while the
   generated data contains the explicitly requested 460. Bronze does not
   reconcile or change defect counts.
3. Local Windows Spark emitted missing `winutils.exe` / native-Hadoop warnings;
   source and metadata validation still completed successfully. This warning
   does not apply to Databricks.

## Final outcome

The implementation defines append-only ingestion for:

| Source | First-run rows | Delta table |
| --- | ---: | --- |
| `data/customers.csv` | 10,000 | `bronze_customers` |
| `data/products.csv` | 500 | `bronze_products` |
| `data/orders.csv` | 100,000 | `bronze_orders` |

All raw source columns and intentional defects are preserved, and the only
added columns are `_ingestion_timestamp` and `_source_file`. No Silver, Gold,
Dashboard, or final-documentation implementation was started.
