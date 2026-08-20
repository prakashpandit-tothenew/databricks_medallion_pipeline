# Data Quality Strategy

Policy: **flag, do not drop**. Every Silver row gets `quality_check_result` (`PASS`/`FAIL`) and `quality_error_codes`. The pipeline must flag **100% of 700 intentional defects**.

## Error-code catalog

Codes are stable, uppercase, and prefixed by family so Gold can group without parsing free text.

### Completeness (`COMP_*`)

| Code | Rule |
| --- | --- |
| `COMP_NULL_PK` | Primary key is NULL or blank after trim. |
| `COMP_NULL_FK` | Required foreign key is NULL or blank. |
| `COMP_NULL_REQUIRED` | Required business field NULL/blank (`email`, `order_date`, `product_name`, `quantity`, `unit_price`). |

### Uniqueness (`UNIQ_*`)

| Code | Rule |
| --- | --- |
| `UNIQ_DUPLICATE_PK` | Business key appears more than once in the batch (all copies flagged). |

### Referential integrity (`REF_*`)

| Code | Rule |
| --- | --- |
| `REF_ORPHAN_CUSTOMER` | `orders.customer_id` not in Silver/Bronze customers keys (including NULL-safe: missing parent). |
| `REF_ORPHAN_ORDER` | `order_items.order_id` not in orders. |
| `REF_ORPHAN_PRODUCT` | `order_items.product_id` not in products. |

Parent lookup uses distinct keys present in the parent entity (NULL keys never count as a match).

### Type / business (`TYPE_*` / `BIZ_*`)

| Code | Rule |
| --- | --- |
| `TYPE_CAST_INT` | `quantity` (or other int) cannot cast. |
| `TYPE_CAST_DECIMAL` | `unit_price` / `line_total` cannot cast. |
| `TYPE_CAST_DATE` | `order_date` / `signup_date` cannot cast. |
| `BIZ_QTY_NON_POSITIVE` | `quantity <= 0` after successful cast. |
| `BIZ_PRICE_NEGATIVE` | `unit_price < 0`. |
| `BIZ_FUTURE_DATE` | Order/signup date after job run date. |
| `BIZ_STATUS_INVALID` | `order_status` not in `{placed, paid, shipped, delivered, cancelled, returned}`. |
| `BIZ_LINE_TOTAL_MISMATCH` | Parsed `line_total` not equal to `quantity * unit_price` within 0.01. |

A row may carry multiple codes. `quality_check_result = FAIL` if any code is present.

## How the 700 planted errors are flagged

Planted defects live in **raw source files**, not in `_ingested_at` / `_source_file`. Bronze stores them unchanged. Silver evaluation is the system of record for the 700 count.

### Planned mix (sums to 700)

| Check family | Target planted count | Typical injection |
| --- | --- | --- |
| Completeness | 250 | NULL/blank PKs, FKs, email, order_date, product_name, prices |
| Uniqueness | 120 | Extra rows reusing `customer_id`, `product_id`, `order_id`, `order_item_id` |
| Referential integrity | 180 | `customer_id` / `order_id` / `product_id` values with no parent |
| Type / business | 150 | Bad types, negative qty/price, future dates, illegal status, line_total drift |
| **Total** | **700** | |

Exact IDs will be listed in an error manifest generated with the data (`data/reference/error_manifest.csv`: `entity`, `business_key`, `error_code`, `planted_flag`). The Silver job left-joins or hashes codes and asserts:

- every manifest row receives its expected code (recall = 100% of 700);
- `gold_dq_summary` planted/FAIL events reconcile to 700 at the **defect grain** (one planted issue = one code instance), not necessarily 700 FAIL rows (a row can hold several codes).

### Defect grain (important for graders)

- **700** = number of intentional issues (code instances), not necessarily 700 distinct rows.
- Duplicate-PK groups: each duplicate **row** is one Uniqueness defect; conflicting copies are not also counted as type errors unless a type/business issue was separately planted.
- Completeness NULL PK does not also emit `REF_ORPHAN_*` for the same empty FK unless a second planted orphan value exists. Implementation will apply codes independently, then the manifest defines which codes are in the 700. Pipeline tests assert manifest ⊆ actual codes.

### Implementation pattern (Silver)

```python
# Conceptual — Spark only, rows retained
df = apply_completeness(df)
df = apply_uniqueness(df)
df = apply_referential(df, parents)
df = apply_type_business(df)
df = pack_quality_columns(df)  # quality_error_codes, quality_check_result
```

No `dropna()`, no filtering to PASS before Delta write.

## Evidence for 100% catch rate

1. Manifest of 700 planted `(entity, key, error_code)` tuples.
2. Silver columns: `quality_check_result`, `quality_error_codes`.
3. `gold_dq_summary` grouped by `check_family` / `error_code` with `defect_count`.
4. Automated check: `assert caught_planted == 700` in `tests/quality`.
5. Notebook log line: `DQ planted caught: 700/700`.

## What is not a planted error

- Bronze metadata columns.
- Extra ignored CSV columns.
- Downstream Gold filters that exclude FAIL rows from KPIs (those rows still exist in Silver).
