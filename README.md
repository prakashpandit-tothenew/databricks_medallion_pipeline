# Databricks Medallion Pipeline

PySpark / Delta Lake e-commerce pipeline: Bronze (raw) → Silver (flagged) → Gold (BI) → SQL dashboard queries.

This README is the runbook for Databricks. Classic Community Edition used Hive + DBFS. This project was also executed on a Unity Catalog workspace (`poc_catalog.default` and a Volume for CSVs). Use the section that matches your workspace.

## Repository layout

```text
docs/                  assessment write-ups (requirements, design, DQ)
ai-prompts/            curated prompt history + per-phase AI records
src/data_generation/   reproducible CSV generator
src/bronze/            CSV → managed Delta ingest (append-only)
src/silver/            DQ flags (no row drops)
src/gold/              business aggregations (Spark SQL)
src/dashboard/         Databricks SQL dashboard queries
data/                  generated datasets (customers, products, orders)
```

**Required submission artefacts**

- Prompt history: [`ai-prompts/PROMPT_HISTORY.md`](ai-prompts/PROMPT_HISTORY.md)
- Generated datasets: [`data/customers.csv`](data/customers.csv), [`data/products.csv`](data/products.csv), [`data/orders.csv`](data/orders.csv)

## Prerequisites

- Databricks cluster or SQL warehouse with PySpark and Delta Lake
- The repo cloned or uploaded under `/Workspace/.../databricks_medallion_pipeline`
- Sample CSVs generated or copied from `data/`

Generate CSVs locally (optional if `data/` is already committed):

```bash
python src/data_generation/generate_sample_data.py
```

Counts: 10,000 customers, 500 products, 100,000 orders. Intentional defects: **460** instances (do not invent more to reach 700).

## 1. Put CSVs where Spark can read them

**Unity Catalog Volume (this assessment’s working path):**

```text
/Volumes/poc_catalog/default/poc_volume/customers.csv
/Volumes/poc_catalog/default/poc_volume/products.csv
/Volumes/poc_catalog/default/poc_volume/orders.csv
```

**Classic Community Edition / DBFS:**

Upload to a folder such as `dbfs:/FileStore/databricks_medallion_pipeline/data/` and pass that folder as `--input-base-path`.

## 2. Attach a cluster and use the notebook `spark` session

Do **not** rely on `python 01_ingest_customers.py` inside a notebook cell (kernel `-f` flags). Call functions instead. After editing files, run:

```python
%restart_python
```

## 3. Bronze (append-only — run once per extract)

Bronze **appends**. Running it twice on the same CSV duplicates keys and makes Silver uniqueness fail.

If you already ingested the same files more than once, drop development tables first (see Reset below).

Notebook (adjust import path to your Workspace copy of `src`):

```python
import sys
sys.path.append("/Workspace/Users/<you>/databricks_medallion_pipeline/src")

from bronze.ingest_all import ingest_all

ingest_all(
    spark,
    input_base_path="/Volumes/poc_catalog/default/poc_volume",
)
```

Expected first-run appends: 10,000 / 500 / 100,000 rows into:

- `poc_catalog.default.bronze_customers`
- `poc_catalog.default.bronze_products`
- `poc_catalog.default.bronze_orders`

On Hive-only CE, tables are `bronze_customers` (no catalog) if you change `BRONZE_TABLE_SCHEMA` in `src/bronze/_common.py`.

## 4. Silver (overwrite — safe to rerun)

```python
from silver.transform_all import transform_all

transform_all(spark)
```

Writes:

- `poc_catalog.default.silver_customers`
- `poc_catalog.default.silver_products`
- `poc_catalog.default.silver_orders`
- `poc_catalog.default.silver_quality_summary`

Invalid rows stay in Silver with `quality_check_result` not equal to `PASSED`.

## 5. Gold (overwrite — safe to rerun)

```python
from gold.create_gold_tables import create_gold_tables

create_gold_tables(
    spark,
    sql_dir="/Workspace/Users/<you>/databricks_medallion_pipeline/src/gold",
)
```

Requires Silver rows with `quality_check_result` in (`PASSED`, `PASS`). Preflight fails if that count is zero.

Tables:

- `poc_catalog.default.gold_sales_by_product`
- `poc_catalog.default.gold_revenue_by_customer`
- `poc_catalog.default.gold_daily_weekly_trends`
- `poc_catalog.default.gold_customer_segmentation`

## 6. Dashboard

1. Open Databricks SQL → create a dashboard.
2. Attach a warehouse that can read `poc_catalog.default`.
3. Paste queries from `src/dashboard/dashboard_queries.sql`.
4. Map:
   - Top 10 products → bar on `total_revenue`
   - Revenue distribution → bar on `customer_count` by `revenue_bucket`
   - Segmentation → pie on `customer_count` by `customer_segment`

## Reset (lab only)

Use only to recover from duplicate Bronze appends of the same extract:

```sql
DROP TABLE IF EXISTS poc_catalog.default.gold_sales_by_product;
DROP TABLE IF EXISTS poc_catalog.default.gold_revenue_by_customer;
DROP TABLE IF EXISTS poc_catalog.default.gold_daily_weekly_trends;
DROP TABLE IF EXISTS poc_catalog.default.gold_customer_segmentation;
DROP TABLE IF EXISTS poc_catalog.default.silver_quality_summary;
DROP TABLE IF EXISTS poc_catalog.default.silver_customers;
DROP TABLE IF EXISTS poc_catalog.default.silver_products;
DROP TABLE IF EXISTS poc_catalog.default.silver_orders;
DROP TABLE IF EXISTS poc_catalog.default.bronze_customers;
DROP TABLE IF EXISTS poc_catalog.default.bronze_products;
DROP TABLE IF EXISTS poc_catalog.default.bronze_orders;
```

Then Bronze **once** → Silver → Gold.

## Documentation index

| File | Contents |
| --- | --- |
| `docs/candidate-info.md` | Candidate template |
| `docs/requirements-analysis.md` | Requirements |
| `docs/design-notes.md` | Architecture |
| `docs/data-quality-strategy.md` | DQ families |
| `tool-workflow.md` | Part A workflow |
| `debugging-notes.md` | Runtime failures |
| `reflection.md` | Reflection |
| `final-ai-usage-summary.md` | AI right/wrong |
| `ai-prompts/PROMPT_HISTORY.md` | Curated prompt history (submission artefact) |
| `ai-prompts/` | Per-phase AI development records |

## Constraints

- PySpark DataFrames for pipeline data (no pandas).
- No hardcoded cloud credentials.
- Bronze append-only; Silver/Gold rebuild with overwrite / `CREATE OR REPLACE`.
