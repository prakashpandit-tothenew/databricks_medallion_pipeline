# Sample Data Generation Notes

## Purpose

`generate_sample_data.py` creates deterministic e-commerce CSV source data for
the Databricks Medallion assessment. This phase produces raw source files only;
it does not implement Bronze, Silver, Gold, or dashboard processing.

## Record counts

| Dataset | Final rows | File |
| --- | ---: | --- |
| Customers | 10,000 | `data/customers.csv` |
| Products | 500 | `data/products.csv` |
| Orders | 100,000 | `data/orders.csv` |

Duplicate defects are introduced by replacing selected IDs with existing IDs,
not by appending rows. Therefore, final row counts remain exact. A duplicate
count means excess rows: `count(non-null ID) - countDistinct(ID)`.

## Schemas

### Customers

| Column | Spark type | Nullable |
| --- | --- | --- |
| `customer_id` | `StringType` | No |
| `customer_name` | `StringType` | No |
| `email` | `StringType` | Yes (50 intentional NULLs) |
| `country` | `StringType` | No |
| `signup_date` | `DateType` | No |
| `customer_segment` | `StringType` | No |
| `lifetime_value` | `DecimalType(12,2)` | No |

### Products

| Column | Spark type | Nullable |
| --- | --- | --- |
| `product_id` | `StringType` | No |
| `product_name` | `StringType` | No |
| `category` | `StringType` | No |
| `price` | `DecimalType(10,2)` | No |
| `cost` | `DecimalType(10,2)` | No |
| `stock_quantity` | `IntegerType` | No |
| `reorder_level` | `IntegerType` | No |

### Orders

| Column | Spark type | Nullable |
| --- | --- | --- |
| `order_id` | `StringType` | No |
| `customer_id` | `StringType` | Yes (100 intentional NULLs) |
| `order_date` | `DateType` | No |
| `product_id` | `StringType` | Yes (200 intentional NULLs) |
| `quantity` | `IntegerType` | No |
| `unit_price` | `DecimalType(10,2)` | No |
| `total_amount` | `DecimalType(12,2)` | No |
| `order_status` | `StringType` | No |
| `payment_date` | `DateType` | No |

## Generation approach

- Fixed default random seed: **20260820**.
- Python's seeded standard-library generator creates values; explicit
  `StructType` schemas create PySpark DataFrames.
- Names, countries, segments, product categories, prices, inventory, statuses,
  and dates use bounded realistic values.
- Valid data is generated before deterministic, disjoint defect selections are
  applied with PySpark column expressions.
- No Faker dependency is required and pandas is not used.
- CSV NULL values are written as empty fields. Files include headers.

## Relationships

- `orders.customer_id` references `customers.customer_id`.
- `orders.product_id` references `products.product_id`.
- `unit_price` comes from the selected product.
- `total_amount = quantity * unit_price`.
- `payment_date` is between zero and three days after `order_date`.
- The ten customer IDs removed to create duplicate IDs are excluded from normal
  order selection. This prevents accidental orphan orders.
- Only the explicitly planted orphan and NULL order keys violate relationships.

## Intentional defects

| Dataset | Defect | Expected instances |
| --- | --- | ---: |
| Customers | NULL `email` | 50 |
| Customers | Duplicate `customer_id` excess rows | 10 |
| Orders | NULL `customer_id` | 100 |
| Orders | NULL `product_id` | 200 |
| Orders | Orphan `customer_id` | 50 |
| Orders | Orphan `product_id` | 30 |
| Orders | Duplicate `order_id` excess rows | 20 |
| **Total** | | **460** |

Selections are non-overlapping within Orders. The injected duplicate rows retain
otherwise valid values, so they do not create extra type or business defects.

## Expected validation

- Customers: 10,000 rows, 50 NULL emails, 10 duplicate-ID excess rows.
- Products: 500 rows, exact columns, positive price/cost, cost below price,
  non-negative sensible inventory.
- Orders: 100,000 rows, 100 NULL customer keys, 200 NULL product keys,
  50 orphan customer keys, 30 orphan product keys, and 20 duplicate-ID excess
  rows.
- All order quantities and monetary values remain positive.
- Every order total equals quantity multiplied by unit price.
- Payment dates are not earlier than order dates.
- Any failed assertion raises `ValueError` before files are saved.

## 460 versus approximately 700 defects

The phase instructions enumerate **460**, not approximately 700, intentional
defect instances. The earlier project strategy describes a future 700-defect
mix, leaving a gap of **240** defects. This generator does not invent those 240
because the phase explicitly prohibits doing so.

Consequently, later documentation and quality assertions that currently expect
700 must be reconciled with the assignment owner before Silver implementation.
For this phase, the authoritative expected total is 460.

## Assumptions

- Source output is a single CSV file per entity under repository-relative
  `data/`.
- Dates are bounded to 2018–2025 for customers and 2023–2025 for orders, making
  generation reproducible independently of the execution date.
- All amounts use one implicit currency.
- A `Pending`, `Cancelled`, or `Returned` record may still have a payment date;
  this avoids introducing unspecified NULL-payment defects.
- The default seed may be overridden for experimentation, but assessment files
  should use the documented default.
- Standalone execution reuses an active Databricks Spark session or creates a
  local Spark session when run outside Databricks.

## Execution

From the repository root:

```bash
python src/data_generation/generate_sample_data.py
```

An alternative repository-relative destination can be supplied with
`--output-dir`.
