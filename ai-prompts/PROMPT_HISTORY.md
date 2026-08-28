# Prompt History (GEN AI Assessment)

Candidate: Prakash Pandit  
Tool: Cursor Agent  
IDE: Cursor (Agent Mode)  
Repo: `databricks_medallion_pipeline`

This is a **curated** record of the prompts that built the project. Duplicate
runtime dumps, Git credential troubleshooting, and low-value follow-ups are
omitted. Per-phase design notes remain in:

- `ai-prompts/data-generation.md`
- `ai-prompts/bronze-layer.md`
- `ai-prompts/silver-layer.md`
- `ai-prompts/gold-layer.md`

Generated datasets (committed under `data/`):

| File | Rows |
| --- | ---: |
| `data/customers.csv` | 10,000 |
| `data/products.csv` | 500 |
| `data/orders.csv` | 100,000 |

Intentional defects planted: **460** (not padded to 700). Seed: `20260820`.

---

## 1. Planning (20 Aug 2026)

**Prompt:** Create planning according to the GEN AI course assessment.

**Outcome:** Scoped Bronze → Silver → Gold → dashboard on Databricks with
PySpark, Delta Lake, explicit schemas, and flag-not-drop data quality.

---

## 2. Cursor rules (20 Aug 2026)

**Prompt:** Create `.cursor/rules/databricks-medallion.mdc` so generated code
follows the architecture (Community Edition / PySpark / Spark SQL / Delta,
medallion layers, no pandas, no schema inference, flag defects instead of
dropping rows, valid Spark types only).

**Outcome:** Persistent project rules used for every later coding turn.

---

## 3. Repository structure and design docs (20 Aug 2026)

**Prompt:** Read the requirements and create the directory structure. Generate:

- `docs/candidate-info.md`
- `docs/requirements-analysis.md`
- `docs/design-notes.md`
- `docs/data-quality-strategy.md` (Completeness, Uniqueness, Referential
  Integrity, Type/Business checks)

**Outcome:** Assessment docs plus a production-style layout:
`docs/`, `src/{data_generation,bronze,silver,gold,dashboard}/`, `data/`,
`ai-prompts/`.

---

## 4. Phase 2 — sample data generation (20 Aug 2026)

**Prompt (condensed):** Implement sample data generation only. Do not start
Bronze/Silver/Gold yet.

- 10,000 customers, 500 products, 100,000 orders
- Realistic columns, fixed seed, modular functions, PySpark DataFrames
- No pandas; repository-relative paths
- Planted defects: 50 NULL emails, 10 duplicate customer IDs, 100 NULL order
  `customer_id`, 200 NULL order `product_id`, 50 orphan customers, 30 orphan
  products, 20 duplicate order IDs
- Do **not** invent extra defects to reach an earlier ~700 estimate
- Validate then write `data/customers.csv`, `data/products.csv`,
  `data/orders.csv`

**Outcome:** `src/data_generation/generate_sample_data.py`, generation notes,
and committed CSVs. Full AI record: `ai-prompts/data-generation.md`.

---

## 5. Phase 3 — Bronze ingest (21 Aug 2026)

**Prompt (condensed):** Ingest the generated CSVs into Delta with explicit
schemas. Files: `01_ingest_customers.py`, `02_ingest_orders.py`,
`03_ingest_products.py`, `ingest_all.py`. Preserve raw rows including
NULLs/duplicates/orphans. Add `_ingestion_timestamp` and `_source_file`.
Append-only. No Silver/Gold yet.

**Outcome:** `src/bronze/` with shared `_common.py`. Full AI record:
`ai-prompts/bronze-layer.md`.

---

## 6. Databricks runtime (how to run Bronze)

These prompts were kept because they defined the **working** workspace, not
because they were stack-trace dumps.

**Prompt:** Explain ingest and where to put CSVs on Databricks.

**Prompt:** CSVs were uploaded to
`/Volumes/poc_catalog/default/poc_volume/customers.csv` (same Volume for
products and orders).

**Prompt:** Which script takes the path; where `BRONZE_BASE_PATH` comes from;
where Delta tables should be written.

**Outcome:** Notebook usage of `ingest_all(spark, input_base_path=...)` against
the Unity Catalog Volume. Managed tables under `poc_catalog.default` (not
external `LOCATION` on Volume/DBFS). Notebook cells call functions; they do
not run `python script.py` (kernel `-f` conflict).

**Runtime adaptations (summarized, logs omitted):**

- Unity Catalog does not support `input_file_name()`; use `_metadata.file_path`.
- Managed UC tables cannot use a Volume/DBFS `LOCATION` (`Missing cloud file
  system scheme`); write with `saveAsTable` and no path.
- Bronze is append-only: ingest the same extract **once**.

---

## 7. Phase 4 — Silver quality (27 Aug 2026)

**Prompt:** Build `src/silver/` with four checks:

- Completeness (email, customer_id, product_id)
- Uniqueness (order_id, customer_id via window functions)
- Referential integrity (left anti-joins)
- Logic/type (dates, positive numerics)

Do not delete bad rows. Add `quality_check_result`. Publish a quality summary
(total / passed / failed / pass %). Save flagged data to Silver Delta tables.
Record the prompt in `ai-prompts/silver-layer.md`.

**Outcome:** `transform_all.py` plus per-entity transforms; tables
`silver_customers`, `silver_products`, `silver_orders`,
`silver_quality_summary`.

---

## 8. Phase 5 — Gold aggregations (27 Aug 2026)

**Prompt:** Create Gold SQL/PySpark:

- `01_sales_by_product.sql`
- `02_revenue_by_customer.sql`
- `03_daily_weekly_trends.sql`
- `04_customer_segmentation.sql`
- `create_gold_tables.py`

Record the prompt in `ai-prompts/gold-layer.md`.

**Outcome:** Gold tables built from Silver rows with
`quality_check_result` in (`PASSED`, `PASS`).

**Runtime adaptations (summarized):**

- Notebooks have no `__file__`; pass
  `sql_dir="/Workspace/Users/<you>/databricks_medallion_pipeline/src/gold"`.
- Empty Gold / zero PASSED customers usually means Bronze was ingested more
  than once (every key looks like a uniqueness fail). Reset tables, ingest
  once, then Silver, then Gold.

---

## 9. Phase 6 — dashboard and submission docs (27 Aug 2026)

**Prompt:** Generate remaining submission files:

- `src/dashboard/dashboard_queries.sql` (top 10 products, revenue histogram,
  segmentation pie, plus supporting queries)
- `tool-workflow.md`
- `debugging-notes.md`
- `reflection.md`
- `final-ai-usage-summary.md`
- Root `README.md` with Databricks run instructions

**Outcome:** BI queries against Gold plus assessment write-ups.

---

## 10. Housekeeping (28 Aug 2026)

**Prompt:** Remove unwanted files/folders.

**Outcome:** Unused empty scaffolding (placeholder `.gitkeep` trees) and
bytecode caches removed. Pipeline source, CSVs, and docs retained.

---

## Intentionally omitted from this history

- Git remote 403 / Credential Manager / `user.name` vs GitHub login
- Repeated paste of the same Databricks exception with full JVM traces
- Duplicate “Bronze FAILED” / “Gold FAILED” messages after the first
  occurrence of each class of issue
- Questions about empty `.gitkeep` placeholders

Those items did not change the product design; the durable fixes are listed
under runtime adaptations above and in `debugging-notes.md`.
