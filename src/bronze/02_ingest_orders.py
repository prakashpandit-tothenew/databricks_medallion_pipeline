"""Append raw Orders CSV rows to the Bronze Delta table.

AI rationale: apply only the typed source contract and Bronze lineage metadata.
Validation: require 100,000 rows and the exact nine source columns while
preserving NULL keys, orphan keys, duplicate IDs, and all other raw values.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

try:
    from ._common import (
        BRONZE_BASE_PATH,
        IngestionResult,
        get_or_create_spark,
        ingest_csv_to_delta,
        join_path,
    )
except ImportError:
    from _common import (  # type: ignore[no-redef]
        BRONZE_BASE_PATH,
        IngestionResult,
        get_or_create_spark,
        ingest_csv_to_delta,
        join_path,
    )

ORDERS_SCHEMA = StructType(
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
ORDERS_COLUMNS = [
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
EXPECTED_ORDER_COUNT = 100_000


def ingest_orders(
    spark: SparkSession,
    source_file: str = "data/orders.csv",
    bronze_base_path: str = BRONZE_BASE_PATH,
    table_name: str = "bronze_orders",
) -> IngestionResult:
    """Ingest Orders using the explicit source schema and append-only Delta."""
    return ingest_csv_to_delta(
        spark=spark,
        source_file=source_file,
        schema=ORDERS_SCHEMA,
        expected_columns=ORDERS_COLUMNS,
        expected_count=EXPECTED_ORDER_COUNT,
        table_name=table_name,
        target_path=join_path(bronze_base_path, "orders"),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest Orders into Bronze Delta.")
    parser.add_argument("--source-file", default="/Volumes/poc_catalog/default/poc_volume/orders.csv")
    parser.add_argument("--bronze-base-path", default=BRONZE_BASE_PATH)
    parser.add_argument("--table-name", default="bronze_orders")
    # Databricks notebooks inject kernel flags such as -f connection.json.
    args, _ = parser.parse_known_args()

    spark, owns_session = get_or_create_spark("bronze-ingest-orders")
    try:
        ingest_orders(
            spark,
            source_file=args.source_file,
            bronze_base_path=args.bronze_base_path,
            table_name=args.table_name,
        )
    finally:
        if owns_session:
            spark.stop()


if __name__ == "__main__":
    main()
