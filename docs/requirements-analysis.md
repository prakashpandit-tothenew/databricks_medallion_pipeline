# Requirements Analysis

Domain: e-commerce order processing. Target: Databricks Community Edition, PySpark, Delta Lake.

## 1. Functional requirements

| ID | Requirement | Layer |
| --- | --- | --- |
| FR-01 | Ingest raw e-commerce files (customers, products, orders, order_items) without mutating source values. | Bronze |
| FR-02 | Stamp every Bronze row with `_ingested_at` and `_source_file`. | Bronze |
| FR-03 | Write Bronze as append-only Delta. | Bronze |
| FR-04 | Apply explicit `StructType` schemas on read; reject runtime schema inference. | All |
| FR-05 | Enforce Silver schemas, standardize types, trim/normalize strings, parse dates. | Silver |
| FR-06 | Deduplicate business keys in Silver while retaining a flagged copy of duplicate rows. | Silver |
| FR-07 | Attach `quality_check_result` and related error-code columns; never drop defect rows. | Silver |
| FR-08 | Detect and flag all 700 planted defects across the four required check families. | Silver |
| FR-09 | Build Gold fact/dimension and KPI tables for BI (revenue, orders, customers, products). | Gold |
| FR-10 | Expose dashboard queries/views: sales KPIs, product performance, and DQ scorecards. | Dashboard |
| FR-11 | Log row counts, defect counts by check type, and write paths after each layer. | All |

### Source entities (logical)

| Entity | Grain | Business key | Required relationships |
| --- | --- | --- | --- |
| `customers` | 1 row per customer | `customer_id` | — |
| `products` | 1 row per SKU | `product_id` | — |
| `orders` | 1 row per order | `order_id` | `customer_id` → `customers` |
| `order_items` | 1 row per order line | `order_item_id` (or `order_id` + `product_id` + line) | `order_id` → `orders`, `product_id` → `products` |

### Expected Gold outputs

- `gold_fct_order_line` — cleaned line-level fact with amounts, flags, and dimension keys.
- `gold_dim_customer`, `gold_dim_product` — current descriptive attributes.
- `gold_kpi_daily_sales` — orders, items, gross revenue, AOV by order date.
- `gold_kpi_product_performance` — units and revenue by product/category.
- `gold_dq_summary` — defect counts by entity, check family, and error code (must total 700 planted issues).

## 2. Non-functional requirements

| ID | Requirement |
| --- | --- |
| NFR-01 | Community Edition single-node: modular functions, no cluster-specific APIs, no UC catalog.names. |
| NFR-02 | Delta as layer-of-record; CSV/JSON only as landing/raw input. |
| NFR-03 | Paths via config (`/dbfs/tmp/medallion/{bronze,silver,gold}/...`), never `C:\` or `/Users/`. |
| NFR-04 | Idempotent Silver/Gold rebuilds (`overwrite` or replace-where); Bronze remains append-only. |
| NFR-05 | Explicit Spark types only (`StringType`, `IntegerType`, `LongType`, `DoubleType`, `DecimalType`, `BooleanType`, `DateType`, `TimestampType`). |
| NFR-06 | Inline documentation in every script: rationale and validation steps. |
| NFR-07 | Deterministic DQ codes so graders can reconcile 700 flags against an error manifest. |
| NFR-08 | Pipeline must run with the notebook `spark` session; do not require pandas. |

## 3. Edge cases

- Duplicate `customer_id` / `order_id` / `product_id` with conflicting attributes.
- NULL or blank primary keys and foreign keys (blanks treated as missing).
- Orphan `orders.customer_id` and `order_items.order_id` / `product_id`.
- Type failures: non-numeric `quantity`/`price`, unparsable dates, invalid booleans.
- Business-rule failures: `quantity <= 0`, `unit_price < 0`, `order_date` in the future, unknown `order_status`, `line_total != quantity * unit_price` (when both parse).
- Same logical defect on one row (multiple codes); row is retained once with a concatenated or array of codes.
- Late-arriving files: Bronze append creates additional `_source_file` versions; Silver prefers latest valid version per key but still flags history/duplicates.
- Empty file or header-only file: log zero rows, do not fail the whole job unless schema cannot be applied.
- Extra unexpected columns in raw files: ignore (not selected into explicit schema).
- Missing expected columns: read as NULL via schema and fail Completeness.

## 4. Assumptions

- Raw files land as CSV (UTF-8, header row) under a configurable DBFS input path.
- Planted defects are in the source extract, not introduced by Bronze metadata columns.
- One pipeline run processes the full current extract (batch), not true CDC.
- “Catch 100% of 700 errors” means every planted defect receives at least one DQ code; Gold DQ totals are reconcilable.
- Dashboard is Databricks SQL / notebook visualizations over Gold tables, not an external BI tool.
- Community Edition has no Unity Catalog; tables are Hive-metastore or path-based Delta.
- Currency is a single implicit currency; no FX conversion.
- Deduplication key for orders is `order_id`; for items, `order_item_id` if present else (`order_id`, `product_id`, `line_number`).
