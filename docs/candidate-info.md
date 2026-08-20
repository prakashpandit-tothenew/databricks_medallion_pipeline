# Candidate Information

Assessment: GEN AI course — Databricks Medallion e-commerce pipeline  
Platform: Databricks Community Edition (single-node, PySpark, Spark SQL, Delta Lake)  
IDE: Cursor (Composer / Agent Mode)

| Parameter | Value |
| --- | --- |
| `CANDIDATE_NAME` | Prakash Pandit |
| `CANDIDATE_ORG` | To The New |
| `GITHUB_USER` | prakashpandit-tothenew |
| `REPO_NAME` | databricks_medallion_pipeline |
| `REPO_URL` | https://github.com/prakashpandit-tothenew/databricks_medallion_pipeline |
| `COURSE_NAME` | GEN AI |
| `ASSESSMENT_TITLE` | Databricks Medallion Data Engineering Pipeline |
| `ASSESSMENT_DATE` | 2026-08-20 |
| `RUNTIME` | Databricks Community Edition |
| `SPARK_API` | PySpark SQL + Spark SQL |
| `PYTHON_VERSION` | 3.10+ |
| `STORAGE_LAYER` | Delta Lake on DBFS (`/dbfs/tmp/medallion/...`) |
| `METASTORE` | Hive metastore (no Unity Catalog three-level names) |
| `DOMAIN` | E-commerce (customers, products, orders, order_items) |
| `INTENTIONAL_DEFECT_COUNT` | 700 |
| `DQ_POLICY` | Flag, do not drop |

## Scope of this submission

- Design and implement a Bronze → Silver → Gold pipeline for e-commerce transactional data.
- Enforce explicit PySpark schemas; do not infer schema in production ingest.
- Flag 100% of 700 planted defects using Completeness, Uniqueness, Referential Integrity, and Type/Business checks.
- Publish Gold aggregates plus a dashboard-ready DQ and sales view.

## Constraints (from project rules)

- PySpark DataFrames only for pipeline transforms (no pandas for big-data operations).
- No hardcoded credentials or absolute local OS paths.
- Bronze is append-only; invalid rows stay in the lake with metadata flags.
