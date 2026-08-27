# AI-Assisted Development: Gold Layer

## Objective

Implement Phase 5 Gold aggregations from Silver PASSED rows as Spark SQL
scripts plus a PySpark runner that persists managed Delta tables.

## Prompt used

The user asked Cursor to create SQL/PySpark aggregation scripts in `src/gold/`:

- `01_sales_by_product.sql`: product_id, product_name, category,
  total_orders, total_revenue, avg_order_value.
- `02_revenue_by_customer.sql`: customer_id, customer_name,
  customer_segment, total_orders, total_revenue, avg_order_value,
  lifetime_value_actual.
- `03_daily_weekly_trends.sql`: revenue over time.
- `04_customer_segmentation.sql`: High-Value, Repeat, One-Time, or
  Inactive metrics.
- `create_gold_tables.py` to execute and persist those Gold Delta tables.
- This actual AI-development history.

## Implementation approach

1. Put each aggregation in a `CREATE OR REPLACE TABLE ... USING DELTA AS`
   Spark SQL file so Databricks notebooks and the SQL editor share one
   definition.
2. Filter `quality_check_result = 'PASSED'` on driving Silver tables so
   defective rows stay in Silver and never enter Gold measures.
3. Persist managed Unity Catalog tables under `poc_catalog.default`, matching
   the working Bronze/Silver environment.
4. Use `create_gold_tables.py` only to load, execute, and log the SQL files.
5. Derive Gold segments from actual PASSED order counts and spend, not by
   copying the source `customer_segment` column alone.

## Cursor suggestions accepted

- Gold overwrite via `CREATE OR REPLACE TABLE` (idempotent rebuilds).
- Distinct order counts for `total_orders` so line-level orders do not inflate
  order volume.
- Left join customers to orders so zero-order PASSED customers remain
  Inactive in segmentation and customer revenue.
- Daily and weekly trends in one table with `period_grain`.
- High-Value threshold of 5000 to match sample-generation LTV floors.
- Notebook-safe `parse_known_args()` and reuse of an active Spark session.

## Suggestions rejected

- pandas aggregations.
- Filtering Silver tables before Gold persistence (Silver must keep FAIL rows).
- Path-based `LOCATION` writes that previously failed Unity Catalog scheme
  checks.
- Implementing a Gold DQ mart in this phase; the prompt scoped four business
  aggregation tables only.

## Manual changes

No manual user edits were reported during implementation.

## Validation performed

- Gold Python compiled successfully.
- Cursor diagnostics reported no linter errors.
- SQL files were reviewed for required output columns and PASSED-only
  measures.
- Managed Delta writes were not executed locally because Unity Catalog is
  Databricks-only. Run `create_gold_tables(spark)` on the workspace after
  Silver tables exist.

## Issues discovered

- Duplicate Silver keys fail uniqueness, so both copies are excluded from
  PASSED Gold inputs. That is consistent with flag-then-filter-for-BI.
- `lifetime_value_actual` is computed spend, which can differ from source
  `lifetime_value`.
- Derived segments can differ from source `customer_segment` because Gold
  recategorizes from observed orders.

## Final outcome

Created:

- `poc_catalog.default.gold_sales_by_product`
- `poc_catalog.default.gold_revenue_by_customer`
- `poc_catalog.default.gold_daily_weekly_trends`
- `poc_catalog.default.gold_customer_segmentation`

No Dashboard implementation was started.
