# Final AI Usage Summary

Tool: Cursor Agent on this repository. Records per phase: `ai-prompts/`.

## What AI got right

- Medallion folder layout and `.cursor/rules` encoding of PySpark/Delta constraints.
- Deterministic sample data with disjoint defect injection and row-count preservation.
- Explicit `StructType` contracts aligned with generated CSVs.
- Silver checks: completeness, window uniqueness, left-anti referential integrity, date/numeric logic, `quality_check_result` packing, metrics table.
- Gold SQL grain (product, customer, daily/weekly, segments) using Silver `PASSED` rows.
- After errors were pasted, targeted fixes: `parse_known_args`, `_metadata.file_path`, managed UC tables, Gold `sql_dir`, Silver preflight.

## What AI got wrong or incomplete on the first try

| Area | Miss | Correction |
| --- | --- | --- |
| Community Edition vs UC | Assumed Hive/DBFS-only | Managed `poc_catalog.default` tables |
| `_ingested_at` vs `_ingestion_timestamp` | Older docs vs Phase 3 prompt | Used Phase 3 column name |
| Bronze path checks | Used `_jvm` filesystem | Spark DataFrame read |
| Lineage | `input_file_name()` | `_metadata.file_path` |
| Delta LOCATION | Volume/DBFS external tables | No `LOCATION` on UC managed tables |
| Gold runner | Relied on `__file__` | Notebook `sql_dir` |
| Gold empty output | No source gate | Fail if zero PASSED customers/orders |
| 700 defects | Early strategy doc | Did not invent 240 extra plants |

## How suggestions were validated

- Python `py_compile` and Cursor diagnostics on new modules.
- Local PySpark (where available) for CSV counts, planted-defect counts, and Silver flag math on `data/*.csv`.
- Databricks job logs: Bronze 10,000 customer rows read; UC error text; Gold 0-row completion; preflight `ValueError` on missing PASSED customers.
- Manual Catalog UI confirmation that CSVs sat under `poc_catalog` / `default` / `poc_volume`.

## Human decisions (not delegated)

- Do not invent defects to reach 700.
- Do not use pandas for pipeline transforms.
- Do not drop invalid rows in Silver.
- Interpret duplicate Bronze batches as a lab operations issue, not a Gold SQL bug.
- Keep dashboard histogram/pie free of customer names.

## Residual risk

End-to-end Gold row counts after a clean Bronze-once → Silver → Gold run were not captured in this chat after the empty-Gold incident. Operators should confirm Gold preflight `passed` counts before submitting dashboard screenshots.
