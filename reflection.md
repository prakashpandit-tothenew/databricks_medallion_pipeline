# Reflection

## What worked

Cursor plus project rules produced a coherent medallion layout: explicit schemas, modular ingest/transform functions, and a consistent “flag, do not drop” Silver policy. Sample generation matched the phase-2 defect list exactly (460 instances) and stayed reproducible with seed `20260820`. Once Unity Catalog managed tables were used, Bronze, Silver, and Gold jobs could run in the same `poc_catalog.default` schema the workspace already had.

## What was harder than the brief

The written assignment assumed Databricks Community Edition without Unity Catalog. The workspace used Volumes (`/Volumes/poc_catalog/default/poc_volume`) and Spark Connect. That gap caused most production failures: `input_file_name`, external Delta paths, and notebook `__file__`. Context-setting in `.cursor/rules` was necessary but not sufficient; runtime errors had to override the original DBFS/Hive assumptions.

## 460 versus approximately 700 defects

Phase 2 forbade inventing extra defects to reach 700. Silver therefore reports observed row-level failures (for example uniqueness flags **both** copies of a duplicate key). Gold and the dashboard must not pretend the planted total is 700 if generation only planted 460.

## Bronze append-only vs lab reruns

Append-only is the correct Bronze contract. In a lab, rerunning the same CSV ingest silently doubles keys and makes Silver uniqueness fail at scale. The operational lesson is: append new files; for a full reload of the same extract, drop or replace Bronze first.

## AI collaboration

AI was strong at scaffolding, schemas, and documenting prompts. It was weak at first-try Databricks serverless/UC specifics until logs were pasted back. Human judgment was required to refuse pandas, refuse invented defects, and to interpret empty Gold as a PASSED-count issue rather than a broken aggregation.

## What I would do next

- Add a Bronze “full refresh” job distinct from append, documented as non-default.
- Persist a small error manifest next to the CSVs for graders.
- Point dashboard tiles at Gold only and keep PII off histogram/pie charts (already started).
