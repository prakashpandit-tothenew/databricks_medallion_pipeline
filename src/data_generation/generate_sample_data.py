"""Generate deterministic e-commerce CSV source data for the assessment.

AI rationale:
    Build valid data first, then inject only the explicitly requested defects
    with disjoint deterministic selections. PySpark DataFrames and explicit
    schemas are used for all validation; pandas and schema inference are not
    used.

Validation:
    ``validate_generated_data`` checks row counts, schemas, numeric/business
    rules, duplicate excess-row counts, NULL counts, and foreign-key defects
    before any output file is written.
"""

from __future__ import annotations

import argparse
import csv
import os
import random
import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Dict, Iterable, Mapping, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

RANDOM_SEED = 20260820
CUSTOMER_COUNT = 10_000
PRODUCT_COUNT = 500
ORDER_COUNT = 100_000

CUSTOMER_NULL_EMAIL_COUNT = 50
CUSTOMER_DUPLICATE_ID_COUNT = 10
ORDER_NULL_CUSTOMER_COUNT = 100
ORDER_NULL_PRODUCT_COUNT = 200
ORDER_ORPHAN_CUSTOMER_COUNT = 50
ORDER_ORPHAN_PRODUCT_COUNT = 30
ORDER_DUPLICATE_ID_COUNT = 20

CUSTOMER_COLUMNS = [
    "customer_id",
    "customer_name",
    "email",
    "country",
    "signup_date",
    "customer_segment",
    "lifetime_value",
]
PRODUCT_COLUMNS = [
    "product_id",
    "product_name",
    "category",
    "price",
    "cost",
    "stock_quantity",
    "reorder_level",
]
ORDER_COLUMNS = [
    "order_id",
    "customer_id",
    "order_date",
    "product_id",
    "quantity",
    "unit_price",
    "total_amount",
    "order_status",
    "payment_date",
]

CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), False),
        StructField("email", StringType(), True),
        StructField("country", StringType(), False),
        StructField("signup_date", DateType(), False),
        StructField("customer_segment", StringType(), False),
        StructField("lifetime_value", DecimalType(12, 2), False),
    ]
)
PRODUCT_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("category", StringType(), False),
        StructField("price", DecimalType(10, 2), False),
        StructField("cost", DecimalType(10, 2), False),
        StructField("stock_quantity", IntegerType(), False),
        StructField("reorder_level", IntegerType(), False),
    ]
)
ORDER_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("order_date", DateType(), False),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DecimalType(10, 2), False),
        StructField("total_amount", DecimalType(12, 2), False),
        StructField("order_status", StringType(), False),
        StructField("payment_date", DateType(), False),
    ]
)

FIRST_NAMES = (
    "Aarav",
    "Aditi",
    "Arjun",
    "Diya",
    "Ishaan",
    "Kavya",
    "Meera",
    "Neha",
    "Rohan",
    "Vikram",
    "Emma",
    "Liam",
    "Olivia",
    "Noah",
    "Sophia",
)
LAST_NAMES = (
    "Sharma",
    "Patel",
    "Singh",
    "Gupta",
    "Mehta",
    "Nair",
    "Reddy",
    "Kumar",
    "Brown",
    "Garcia",
    "Miller",
    "Wilson",
)
COUNTRIES = (
    "India",
    "United States",
    "United Kingdom",
    "Germany",
    "Australia",
    "Canada",
    "Singapore",
    "United Arab Emirates",
)
CUSTOMER_SEGMENTS = ("High-Value", "Repeat", "One-Time", "Inactive")
SEGMENT_WEIGHTS = (0.12, 0.38, 0.35, 0.15)
ORDER_STATUSES = ("Pending", "Completed", "Cancelled", "Returned")
ORDER_STATUS_WEIGHTS = (0.12, 0.72, 0.10, 0.06)
CATEGORY_PRODUCTS = {
    "Electronics": ("Wireless Headphones", "Smart Watch", "Bluetooth Speaker", "Webcam"),
    "Home": ("Table Lamp", "Storage Basket", "Coffee Maker", "Throw Pillow"),
    "Books": ("Data Engineering Guide", "Business Strategy", "Modern Fiction", "Cookbook"),
    "Clothing": ("Cotton T-Shirt", "Denim Jacket", "Running Shoes", "Wool Sweater"),
    "Beauty": ("Face Cleanser", "Body Lotion", "Hair Serum", "Sunscreen"),
    "Sports": ("Yoga Mat", "Resistance Bands", "Water Bottle", "Training Gloves"),
}


def _customer_id(number: int) -> str:
    return f"CUST-{number:05d}"


def _product_id(number: int) -> str:
    return f"PROD-{number:04d}"


def _order_id(number: int) -> str:
    return f"ORD-{number:06d}"


def _quantize(value: float) -> Decimal:
    return Decimal(str(value)).quantize(Decimal("0.01"))


def _choose_ids(
    ids: Sequence[str], count: int, seed: int, excluded: Iterable[str] = ()
) -> list[str]:
    """Select stable, unique IDs while respecting prior defect selections."""
    excluded_set = set(excluded)
    available = [value for value in ids if value not in excluded_set]
    return random.Random(seed).sample(available, count)


def _customer_defect_plan(seed: int) -> Tuple[list[str], Dict[str, str]]:
    all_ids = [_customer_id(index) for index in range(1, CUSTOMER_COUNT + 1)]
    null_email_ids = _choose_ids(all_ids, CUSTOMER_NULL_EMAIL_COUNT, seed + 101)
    duplicate_targets = _choose_ids(
        all_ids,
        CUSTOMER_DUPLICATE_ID_COUNT,
        seed + 102,
        excluded=null_email_ids,
    )
    duplicate_sources = _choose_ids(
        all_ids,
        CUSTOMER_DUPLICATE_ID_COUNT,
        seed + 103,
        excluded=[*null_email_ids, *duplicate_targets],
    )
    return null_email_ids, dict(zip(duplicate_targets, duplicate_sources))


def _order_defect_plan(seed: int) -> Dict[str, object]:
    """Create non-overlapping order selections so exactly 400 issues are injected."""
    all_ids = [_order_id(index) for index in range(1, ORDER_COUNT + 1)]
    rng = random.Random(seed + 201)
    rng.shuffle(all_ids)
    cursor = 0

    def take(count: int) -> list[str]:
        nonlocal cursor
        result = all_ids[cursor : cursor + count]
        cursor += count
        return result

    null_customer_ids = take(ORDER_NULL_CUSTOMER_COUNT)
    null_product_ids = take(ORDER_NULL_PRODUCT_COUNT)
    orphan_customer_ids = take(ORDER_ORPHAN_CUSTOMER_COUNT)
    orphan_product_ids = take(ORDER_ORPHAN_PRODUCT_COUNT)
    duplicate_targets = take(ORDER_DUPLICATE_ID_COUNT)
    duplicate_sources = take(ORDER_DUPLICATE_ID_COUNT)
    return {
        "null_customer": null_customer_ids,
        "null_product": null_product_ids,
        "orphan_customer": orphan_customer_ids,
        "orphan_product": orphan_product_ids,
        "duplicate_replacements": dict(zip(duplicate_targets, duplicate_sources)),
    }


def generate_customers(
    spark: SparkSession, seed: int = RANDOM_SEED
) -> DataFrame:
    """Generate 10,000 valid customer rows using an explicit Spark schema."""
    rng = random.Random(seed)
    start_date = date(2018, 1, 1)
    end_date = date(2025, 12, 31)
    date_span = (end_date - start_date).days
    rows = []

    for index in range(1, CUSTOMER_COUNT + 1):
        first_name = rng.choice(FIRST_NAMES)
        last_name = rng.choice(LAST_NAMES)
        customer_id = _customer_id(index)
        segment = rng.choices(CUSTOMER_SEGMENTS, weights=SEGMENT_WEIGHTS, k=1)[0]
        lifetime_ranges = {
            "High-Value": (5_000, 25_000),
            "Repeat": (800, 7_500),
            "One-Time": (20, 1_200),
            "Inactive": (0, 500),
        }
        low, high = lifetime_ranges[segment]
        rows.append(
            (
                customer_id,
                f"{first_name} {last_name}",
                f"{first_name}.{last_name}.{index}@example.com".lower(),
                rng.choice(COUNTRIES),
                start_date + timedelta(days=rng.randint(0, date_span)),
                segment,
                _quantize(rng.uniform(low, high)),
            )
        )

    return spark.createDataFrame(rows, schema=CUSTOMER_SCHEMA)


def generate_products(
    spark: SparkSession, seed: int = RANDOM_SEED
) -> DataFrame:
    """Generate 500 valid products with sensible price and inventory values."""
    rng = random.Random(seed + 1)
    rows = []
    categories = tuple(CATEGORY_PRODUCTS)

    for index in range(1, PRODUCT_COUNT + 1):
        category = rng.choice(categories)
        base_name = rng.choice(CATEGORY_PRODUCTS[category])
        price = _quantize(rng.uniform(8.0, 1_500.0))
        cost = _quantize(float(price) * rng.uniform(0.35, 0.78))
        stock_quantity = rng.randint(20, 1_000)
        reorder_level = rng.randint(5, min(150, stock_quantity))
        rows.append(
            (
                _product_id(index),
                f"{base_name} {index:03d}",
                category,
                price,
                cost,
                stock_quantity,
                reorder_level,
            )
        )

    return spark.createDataFrame(rows, schema=PRODUCT_SCHEMA)


def generate_orders(
    spark: SparkSession,
    customer_ids: Sequence[str],
    product_prices: Mapping[str, Decimal],
    seed: int = RANDOM_SEED,
) -> DataFrame:
    """Generate 100,000 valid orders related to the supplied parent keys."""
    rng = random.Random(seed + 2)
    product_ids = tuple(product_prices)
    order_start = date(2023, 1, 1)
    order_end = date(2025, 12, 31)
    date_span = (order_end - order_start).days
    rows = []

    for index in range(1, ORDER_COUNT + 1):
        product_id = rng.choice(product_ids)
        quantity = rng.randint(1, 8)
        unit_price = product_prices[product_id]
        order_date = order_start + timedelta(days=rng.randint(0, date_span))
        payment_date = order_date + timedelta(days=rng.randint(0, 3))
        rows.append(
            (
                _order_id(index),
                rng.choice(customer_ids),
                order_date,
                product_id,
                quantity,
                unit_price,
                (unit_price * quantity).quantize(Decimal("0.01")),
                rng.choices(ORDER_STATUSES, weights=ORDER_STATUS_WEIGHTS, k=1)[0],
                payment_date,
            )
        )

    return spark.createDataFrame(rows, schema=ORDER_SCHEMA)


def _replace_ids(
    dataframe: DataFrame,
    id_column: str,
    replacements: Mapping[str, str],
) -> DataFrame:
    """Replace selected IDs without changing the final row count."""
    mapping_items = [
        literal
        for old_id, new_id in replacements.items()
        for literal in (F.lit(old_id), F.lit(new_id))
    ]
    replacement_map = F.create_map(*mapping_items)
    return dataframe.withColumn(
        id_column,
        F.coalesce(F.element_at(replacement_map, F.col(id_column)), F.col(id_column)),
    )


def inject_customer_defects(
    customers: DataFrame, seed: int = RANDOM_SEED
) -> DataFrame:
    """Inject exactly 50 NULL emails and 10 duplicate-ID excess rows."""
    null_email_ids, duplicate_replacements = _customer_defect_plan(seed)
    defective = customers.withColumn(
        "email",
        F.when(F.col("customer_id").isin(null_email_ids), F.lit(None).cast(StringType()))
        .otherwise(F.col("email")),
    )
    return _replace_ids(defective, "customer_id", duplicate_replacements)


def inject_order_defects(
    orders: DataFrame, seed: int = RANDOM_SEED
) -> DataFrame:
    """Inject only the five explicitly requested order defect types."""
    plan = _order_defect_plan(seed)
    defective = (
        orders.withColumn(
            "customer_id",
            F.when(
                F.col("order_id").isin(plan["null_customer"]),
                F.lit(None).cast(StringType()),
            )
            .when(
                F.col("order_id").isin(plan["orphan_customer"]),
                F.concat(F.lit("ORPHAN-CUST-"), F.col("order_id")),
            )
            .otherwise(F.col("customer_id")),
        )
        .withColumn(
            "product_id",
            F.when(
                F.col("order_id").isin(plan["null_product"]),
                F.lit(None).cast(StringType()),
            )
            .when(
                F.col("order_id").isin(plan["orphan_product"]),
                F.concat(F.lit("ORPHAN-PROD-"), F.col("order_id")),
            )
            .otherwise(F.col("product_id")),
        )
    )
    return _replace_ids(
        defective,
        "order_id",
        plan["duplicate_replacements"],
    )


def _duplicate_excess_count(dataframe: DataFrame, id_column: str) -> int:
    counts = dataframe.agg(
        F.count(F.col(id_column)).alias("non_null_count"),
        F.countDistinct(F.col(id_column)).alias("distinct_count"),
    ).first()
    return counts["non_null_count"] - counts["distinct_count"]


def _orphan_count(
    child: DataFrame,
    child_key: str,
    parent: DataFrame,
    parent_key: str,
) -> int:
    parent_keys = (
        parent.select(F.col(parent_key).alias(child_key))
        .where(F.col(parent_key).isNotNull())
        .distinct()
    )
    return (
        child.select(child_key)
        .where(F.col(child_key).isNotNull())
        .join(parent_keys, child_key, "left_anti")
        .count()
    )


def validate_generated_data(
    customers: DataFrame,
    products: DataFrame,
    orders: DataFrame,
) -> Dict[str, int]:
    """Validate all assessment counts and raise a clear error on any mismatch."""
    customers.cache()
    products.cache()
    orders.cache()

    metrics = {
        "customer_rows": customers.count(),
        "customer_null_emails": customers.where(F.col("email").isNull()).count(),
        "customer_duplicate_ids": _duplicate_excess_count(customers, "customer_id"),
        "product_rows": products.count(),
        "invalid_products": products.where(
            (F.col("price") <= 0)
            | (F.col("cost") <= 0)
            | (F.col("cost") >= F.col("price"))
            | (F.col("stock_quantity") < 0)
            | (F.col("reorder_level") < 0)
            | (F.col("reorder_level") > F.col("stock_quantity"))
        ).count(),
        "order_rows": orders.count(),
        "order_null_customers": orders.where(F.col("customer_id").isNull()).count(),
        "order_null_products": orders.where(F.col("product_id").isNull()).count(),
        "order_duplicate_ids": _duplicate_excess_count(orders, "order_id"),
        "order_orphan_customers": _orphan_count(
            orders, "customer_id", customers, "customer_id"
        ),
        "order_orphan_products": _orphan_count(
            orders, "product_id", products, "product_id"
        ),
        "order_unit_price_mismatches": (
            orders.alias("orders")
            .join(products.alias("products"), "product_id", "inner")
            .where(F.col("orders.unit_price") != F.col("products.price"))
            .count()
        ),
        "order_total_mismatches": orders.where(
            F.abs(F.col("total_amount") - (F.col("quantity") * F.col("unit_price")))
            > F.lit(Decimal("0.01"))
        ).count(),
        "invalid_order_dates": orders.where(
            (F.col("payment_date").isNull())
            | (F.col("payment_date") < F.col("order_date"))
        ).count(),
        "invalid_order_values": orders.where(
            (F.col("quantity") <= 0)
            | (F.col("unit_price") <= 0)
            | (F.col("total_amount") <= 0)
            | (~F.col("order_status").isin(*ORDER_STATUSES))
        ).count(),
    }

    errors = []

    def expect(metric: str, expected: int) -> None:
        actual = metrics[metric]
        if actual != expected:
            errors.append(f"{metric}: expected {expected}, got {actual}")

    expect("customer_rows", CUSTOMER_COUNT)
    expect("customer_null_emails", CUSTOMER_NULL_EMAIL_COUNT)
    expect("customer_duplicate_ids", CUSTOMER_DUPLICATE_ID_COUNT)
    expect("product_rows", PRODUCT_COUNT)
    expect("invalid_products", 0)
    expect("order_rows", ORDER_COUNT)
    expect("order_null_customers", ORDER_NULL_CUSTOMER_COUNT)
    expect("order_null_products", ORDER_NULL_PRODUCT_COUNT)
    expect("order_orphan_customers", ORDER_ORPHAN_CUSTOMER_COUNT)
    expect("order_orphan_products", ORDER_ORPHAN_PRODUCT_COUNT)
    expect("order_duplicate_ids", ORDER_DUPLICATE_ID_COUNT)
    expect("order_unit_price_mismatches", 0)
    expect("order_total_mismatches", 0)
    expect("invalid_order_dates", 0)
    expect("invalid_order_values", 0)

    if list(customers.columns) != CUSTOMER_COLUMNS:
        errors.append(f"customer columns do not match: {customers.columns}")
    if list(products.columns) != PRODUCT_COLUMNS:
        errors.append(f"product columns do not match: {products.columns}")
    if list(orders.columns) != ORDER_COLUMNS:
        errors.append(f"order columns do not match: {orders.columns}")

    if errors:
        print("Validation failed:")
        for error in errors:
            print(f"  - {error}")
        raise ValueError("Generated data failed validation")

    print(
        "Validation passed | "
        f"customers={metrics['customer_rows']:,} "
        f"(NULL email={metrics['customer_null_emails']}, "
        f"duplicate IDs={metrics['customer_duplicate_ids']}) | "
        f"products={metrics['product_rows']:,} | "
        f"orders={metrics['order_rows']:,} "
        f"(NULL customer={metrics['order_null_customers']}, "
        f"NULL product={metrics['order_null_products']}, "
        f"orphan customer={metrics['order_orphan_customers']}, "
        f"orphan product={metrics['order_orphan_products']}, "
        f"duplicate IDs={metrics['order_duplicate_ids']})"
    )
    return metrics


def _csv_value(value: object) -> object:
    if value is None:
        return ""
    if isinstance(value, (date, Decimal)):
        return str(value)
    return value


def _write_csv(dataframe: DataFrame, output_file: Path) -> None:
    """Write one header-bearing CSV file; empty fields preserve SQL NULLs."""
    output_file.parent.mkdir(parents=True, exist_ok=True)
    temporary_file = output_file.with_suffix(".csv.tmp")
    with temporary_file.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(dataframe.columns)
        # Sort every column so duplicate-ID ties are byte-for-byte reproducible.
        for row in dataframe.orderBy(*dataframe.columns).toLocalIterator():
            writer.writerow([_csv_value(value) for value in row])
    temporary_file.replace(output_file)


def save_data(
    customers: DataFrame,
    products: DataFrame,
    orders: DataFrame,
    output_dir: Path | str = Path("data"),
) -> None:
    """Save exact repository-relative CSV files after validation succeeds."""
    destination = Path(output_dir)
    _write_csv(customers, destination / "customers.csv")
    _write_csv(products, destination / "products.csv")
    _write_csv(orders, destination / "orders.csv")
    print(f"Saved CSV files under {destination}")


def _get_spark() -> Tuple[SparkSession, bool]:
    """Reuse Databricks' active session or create one for standalone execution."""
    active_session = SparkSession.getActiveSession()
    if active_session is not None:
        return active_session, False
    # Spark's Windows fallback can otherwise resolve `python3` to an app alias
    # instead of the interpreter running this script, resetting worker sockets.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    session = (
        SparkSession.builder.appName("databricks-medallion-sample-data")
        # A single local worker mirrors the Community Edition constraint and
        # avoids Windows Python-worker socket instability during local checks.
        .master("local[1]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.pyspark.python", sys.executable)
        .getOrCreate()
    )
    return session, True


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate deterministic e-commerce assessment CSV data."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data"),
        help="Repository-relative output directory (default: data).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=RANDOM_SEED,
        help=f"Deterministic random seed (default: {RANDOM_SEED}).",
    )
    args = parser.parse_args()

    spark, owns_session = _get_spark()
    spark.sparkContext.setLogLevel("WARN")
    try:
        valid_customers = generate_customers(spark, args.seed)
        products = generate_products(spark, args.seed)

        # Avoid customer IDs that duplicate injection will remove. This keeps
        # every non-planted order relationship valid after customer injection.
        _, customer_replacements = _customer_defect_plan(args.seed)
        surviving_customer_ids = [
            _customer_id(index)
            for index in range(1, CUSTOMER_COUNT + 1)
            if _customer_id(index) not in customer_replacements
        ]
        product_prices = {
            row["product_id"]: row["price"]
            for row in products.select("product_id", "price").collect()
        }
        valid_orders = generate_orders(
            spark,
            customer_ids=surviving_customer_ids,
            product_prices=product_prices,
            seed=args.seed,
        )

        customers = inject_customer_defects(valid_customers, args.seed)
        orders = inject_order_defects(valid_orders, args.seed)
        validate_generated_data(customers, products, orders)
        save_data(customers, products, orders, args.output_dir)
    finally:
        if owns_session:
            spark.stop()


if __name__ == "__main__":
    main()
