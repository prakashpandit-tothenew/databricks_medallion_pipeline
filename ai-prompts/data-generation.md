# AI-Assisted Development: Sample Data Generation

## Objective

Implement Phase 2 sample data generation for the Databricks Medallion
assessment, producing reproducible Customers, Products, and Orders CSV sources
with exactly the specified intentional defects. Bronze, Silver, Gold,
Dashboard, and final assessment documentation are explicitly out of scope.

## Prompt used

The user instructed Cursor to:

> Implement Phase 2: Sample Data Generation for the Databricks Medallion
> Pipeline. Follow the existing project rules, `requirements-analysis.md`,
> `design-notes.md`, and `data-quality-strategy.md`.
>
> Generate exactly 10,000 customers, 500 products, and 100,000 orders with the
> listed e-commerce columns and valid normal relationships. Use a fixed seed,
> modular functions, PySpark DataFrames where appropriate, repository-relative
> paths, no pandas, and no hardcoded credentials or absolute local paths.
>
> Inject exactly 50 NULL customer emails, 10 duplicate customer IDs, 100 NULL
> order customer IDs, 200 NULL order product IDs, 50 orphan customer IDs, 30
> orphan product IDs, and 20 duplicate order IDs. Do not add the unspecified
> 240 defects needed to reach the earlier approximate 700 total; document the
> discrepancy.
>
> Validate all row, column, relationship, defect, amount, and date expectations
> before saving `data/customers.csv`, `data/products.csv`, and
> `data/orders.csv`. Create generation notes and this AI-development record,
> then stop without implementing downstream layers.

This is a faithful condensed record of the supplied prompt; no additional
feature request was inferred.

## Implementation approach suggested by Cursor

Cursor proposed:

1. Read the existing project rules and all three referenced design documents.
2. Define explicit `StructType` schemas and fixed constants for counts and seed.
3. Generate valid deterministic rows first using the Python standard random
   generator, then create PySpark DataFrames.
4. Create deterministic, non-overlapping ID selections for each defect type.
5. Preserve exact final row counts by replacing selected IDs with existing IDs
   for duplicate defects rather than appending rows.
6. Exclude customer IDs that will be removed by duplicate replacement from
   normal order selection, preventing accidental orphan defects.
7. Validate with Spark aggregations and anti-joins before writing.
8. Write one header-bearing CSV per entity with empty fields for SQL NULLs.

## Suggestions accepted

- Explicit PySpark schemas and no schema inference.
- Standard-library synthetic values instead of adding Faker.
- Fixed default seed `20260820`.
- Disjoint order defect selections to prevent overlap and ambiguous counts.
- Duplicate count defined as `count(non-null ID) - countDistinct(ID)`.
- Replacement-based duplicates to retain exact 10,000/100,000 final row counts.
- Spark left-anti joins for exact orphan validation.
- Validation failure raises `ValueError` before saving.
- A local Spark fallback for independent execution while reusing Databricks'
  active session when present.

## Suggestions rejected

- **Appending duplicate rows:** rejected because it would produce 10,010
  customers and 100,020 orders, violating exact final counts.
- **Faker:** not used because realistic deterministic values can be generated
  without adding a dependency.
- **pandas:** rejected by the project rules and unnecessary for this volume.
- **Inventing 240 more defects:** rejected because the prompt explicitly
  prohibits it.
- **Implementing an error manifest or Silver flags now:** deferred because they
  belong to later phases and the user required this phase to stop after source
  generation.

## Manual changes

No manual user edits were reported during this phase. Cursor authored the
generator and documentation directly from the supplied requirements.

## Validation performed

- Python syntax compilation completed successfully.
- Cursor diagnostics reported no linter errors.
- A full PySpark run passed all in-script count, schema, amount, date, numeric,
  duplicate, and foreign-key checks.
- A separate standard-library CSV inspection confirmed the emitted row counts,
  headers, NULL counts, duplicate excess counts, and orphan counts.

## Issues discovered

1. The detailed defect list totals 460, while earlier documents describe
   approximately 700 defects. The missing 240 were not invented.
2. Fixed row counts make “duplicate records” ambiguous. This implementation
   defines the requested 10/20 values as duplicate excess rows while retaining
   exact final counts.
3. Replacing customer IDs can remove parent keys and accidentally orphan
   otherwise valid orders. Order generation therefore excludes those ten
   soon-to-be-removed IDs.
4. PySpark was not initially installed in the local development environment;
   Java-8-compatible PySpark 3.5.9 was installed for local verification.
   Databricks supplies PySpark in the target runtime.
5. The first local runs exposed Windows Python-worker socket resets and a
   PySpark tuple-literal incompatibility. The standalone fallback was limited
   to one worker with the current interpreter explicitly configured, and
   `isin` was changed to receive expanded status values. The final run passed.

## Final outcome

The implementation produced:

- 10,000 Customers: 50 NULL emails and 10 duplicate-ID excess rows.
- 500 valid Products.
- 100,000 Orders: 100 NULL customer keys, 200 NULL product keys, 50 orphan
  customer keys, 30 orphan product keys, and 20 duplicate-ID excess rows.

The final generation process exited successfully, all validation assertions
passed, and `data/customers.csv`, `data/products.csv`, and `data/orders.csv`
were written. The intentional-defect total is 460. No Bronze ingestion or
downstream phase was started.
