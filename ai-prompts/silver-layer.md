# AI-Assisted Development: Silver Layer

## Objective

Implement Phase 4 Silver transformations for Customers, Products, and Orders
with four quality-check families, row-level flags, per-check metrics, managed
Delta outputs, and no deletion of defective Bronze rows.

## Prompt used

The user asked Cursor to build PySpark scripts in `src/silver/` implementing:

- Completeness checks for critical `email`, `customer_id`, and `product_id`
  fields.
- Uniqueness checks for `customer_id` and `order_id` using window functions.
- Referential-integrity checks for order customer/product keys using left
  anti-joins.
- Logic/type checks for dates and positive numeric values.
- A `quality_check_result` column such as `PASSED` or
  `FAILED_COMPLETENESS`.
- A quality summary with total, passed, failed, and pass percentage per check.
- Silver Delta tables that retain rather than delete bad rows.
- This actual AI-development history.

## Implementation approach

1. Added `_common.py` for schema enforcement, strict Boolean checks, packed
   quality statuses/codes, metrics, managed Delta writes, logging, and row-count
   preservation.
2. Added separate Customers, Products, and Orders transformations with explicit
   Spark schemas matching Bronze source types.
3. Applied `Window.partitionBy(...).count()` for duplicate customer/order keys.
4. Created distinct orphan-key sets with `left_anti` joins, then joined marker
   columns back to Orders so no rows were removed.
5. Added applicable per-row pass columns:
   - `dq_completeness_pass`
   - `dq_uniqueness_pass`
   - `dq_referential_integrity_pass`
   - `dq_logic_type_pass`
6. Added `quality_check_result` and `quality_error_codes`, supporting multiple
   simultaneous failures separated by semicolons.
7. Overwrote managed Silver tables for idempotent full-batch rebuilds and wrote
   the combined `silver_quality_summary` table.

## Cursor suggestions accepted

- Preserve every Bronze row in the corresponding Silver table.
- Flag every row in a duplicate-key group rather than retaining only one copy.
- Treat NULL foreign keys as Completeness failures, not additional orphan
  failures.
- Use managed Unity Catalog tables under `poc_catalog.default`, matching the
  working Bronze environment.
- Use overwrite only for Silver and metrics, never for append-only Bronze.
- Include total/passed/failed/pass-percentage metrics for each applicable check.
- Check order total consistency in addition to positive numeric/date checks.

## Suggestions rejected

- Dropping, filtering, or deduplicating bad rows was rejected because this phase
  explicitly requires flagging.
- pandas and Python-side row processing were rejected; transformations remain
  PySpark-only.
- Order-status validation was not added because this prompt specifically scopes
  Logic/Type to dates and positive numeric values, and existing documents use a
  status vocabulary different from generated data.
- Products do not receive artificial Uniqueness or Referential Integrity
  metrics because those checks are not applicable to the requested product
  contract.

## Manual changes

No manual user edits were reported during implementation.

## Validation performed

- All Silver Python modules compiled successfully.
- Cursor diagnostics reported no linter errors.
- A prohibited-pattern scan found no pandas, schema inference, row filters,
  `dropna`, or `dropDuplicates`.
- Controlled local PySpark data verified:
  - Source and Silver row counts were equal for all three datasets.
  - Customer failures: Completeness 1, Uniqueness 2, Logic/Type 0.
  - Product failures: Completeness 0, Logic/Type 1.
  - Order failures: Completeness 1, Uniqueness 2,
    Referential Integrity 2, Logic/Type 1.
- Full generated CSV validation preserved all source rows and reported:
  - Customers (10,000): Completeness 50, Uniqueness 20, Logic/Type 0.
  - Products (500): Completeness 0, Logic/Type 0.
  - Orders (100,000): Completeness 300, Uniqueness 40,
    Referential Integrity 80, Logic/Type 0.
- Managed Delta writes were not executed locally because the local PySpark
  runtime does not provide Databricks Unity Catalog. The write paths use
  `saveAsTable` and are intended for the existing Databricks workspace.

## Issues discovered

- Duplicate counts are row-level. Ten duplicated customer IDs produce twenty
  failed Uniqueness rows because both copies are flagged.
- Earlier project documentation targets approximately 700 defects, while Phase
  2 generated the explicitly requested 460 defect instances. Silver reports
  observed row-level check failures and does not invent missing defects.
- The active Databricks environment supports Unity Catalog despite earlier
  Community Edition assumptions, so managed three-part table names are used.

## Final outcome

Created transformations for:

- `poc_catalog.default.bronze_customers` →
  `poc_catalog.default.silver_customers`
- `poc_catalog.default.bronze_products` →
  `poc_catalog.default.silver_products`
- `poc_catalog.default.bronze_orders` →
  `poc_catalog.default.silver_orders`
- Combined metrics →
  `poc_catalog.default.silver_quality_summary`

No Gold or Dashboard implementation was started.
