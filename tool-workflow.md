# Tool Workflow (Part A)

Assessment: GEN AI — Databricks Medallion e-commerce pipeline  
Candidate: Prakash Pandit  
IDE: Cursor (Agent Mode)

## AI context-setting

Persistent context was set in `.cursor/rules/databricks-medallion.mdc` so every coding turn used the same stack (PySpark, Delta, explicit schemas, flag-not-drop, no pandas).

Each phase started from repository documents, not from a blank chat:

| Phase | Primary context |
| --- | --- |
| Planning | Assignment brief, medallion rules |
| Data generation | `docs/requirements-analysis.md`, `docs/data-quality-strategy.md` |
| Bronze | Generated CSV contracts, Community Edition / DBFS notes |
| Silver | Bronze UC table names, four DQ families |
| Gold | Silver `quality_check_result`, Gold SQL specs |
| Dashboard / docs | Gold table schemas and actual Databricks errors |

Phase-specific AI records live in `ai-prompts/`.

## Validation strategies

- **Contract counts:** 10,000 customers, 500 products, 100,000 orders before any lake write.
- **Defect grain:** planted issues counted as specified (460 in generation; do not invent 240 to reach 700).
- **Spark-only checks:** no `toPandas()` for pipeline validation.
- **Layer gates:** Bronze preserves source rows; Silver preserves Bronze counts and flags defects; Gold uses only `PASSED` / `PASS` rows.
- **Runtime proof:** Databricks logs for ingest, Silver metrics, and Gold preflight counts.
- **Regression:** after Unity Catalog failures (`input_file_name`, Volume `LOCATION`, notebook `__file__`), re-run the same job and require a new error class or a success log.

## Debugging logs (representative)

| Symptom | Cause | Fix |
| --- | --- | --- |
| `unrecognized arguments: -f .../connection.json` | Notebook kernel flags vs `argparse.parse_args()` | `parse_known_args()` |
| JVM / Hadoop path check failed in 0.00s | `spark.sparkContext._jvm` unavailable on Spark Connect | `spark.read.text` existence check |
| `input_file_name` not supported in Unity Catalog | UC blocks that function | `_metadata.file_path` |
| `Missing cloud file system scheme` | `saveAsTable` + `dbfs:/tmp` or Volume `LOCATION` | Managed UC tables, no external path |
| `__file__` is not defined | Gold runner executed as a notebook | Resolve `sql_dir` / search `src/gold` |
| All Gold tables 0 rows | `quality_check_result` never `PASSED` | Preflight; do not re-append Bronze |

Do not paste secrets, tokens, or personal data into logs. Synthetic emails in sample CSVs are fake (`@example.com`).

## Privacy handling

- No cloud credentials, personal passwords, or API keys in the repo.
- Paths are Volume / UC names (`/Volumes/poc_catalog/...`, `poc_catalog.default.*`), not laptop home directories in job config.
- Dashboard histogram uses revenue buckets, not customer names.
- Git remotes and `user.email` stay in git config, not in pipeline code.
- Do not commit `.env` or credential files (none are required for this pipeline).

## Lessons learned

1. Cursor rules stop many schema/pandas mistakes; they do not replace the target runtime (Unity Catalog vs classic Hive/DBFS).
2. Append-only Bronze is correct for the lake, but repeated ingest without a new file batch duplicates keys and fails every uniqueness check.
3. Databricks notebooks are not `python script.py`; argparse and `__file__` must be notebook-safe.
4. Gold empty output is often a Silver PASSED-count problem, not a broken `GROUP BY`.
5. Document the 460 vs ~700 defect gap instead of inventing extra errors.
