"""Append raw Products CSV rows to the Bronze Delta table.

AI rationale: keep product ingestion schema-explicit and free of business
cleanup. Validation: require 500 rows and the exact seven source columns;
inventory and pricing quality belongs to Silver, not Bronze.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import (
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

PRODUCTS_SCHEMA = StructType(
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
PRODUCTS_COLUMNS = [
    "product_id",
    "product_name",
    "category",
    "price",
    "cost",
    "stock_quantity",
    "reorder_level",
]
EXPECTED_PRODUCT_COUNT = 500


def ingest_products(
    spark: SparkSession,
    source_file: str = "data/products.csv",
    bronze_base_path: str = BRONZE_BASE_PATH,
    table_name: str = "bronze_products",
) -> IngestionResult:
    """Ingest Products using the explicit source schema and append-only Delta."""
    return ingest_csv_to_delta(
        spark=spark,
        source_file=source_file,
        schema=PRODUCTS_SCHEMA,
        expected_columns=PRODUCTS_COLUMNS,
        expected_count=EXPECTED_PRODUCT_COUNT,
        table_name=table_name,
        target_path=join_path(bronze_base_path, "products"),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest Products into Bronze Delta.")
    parser.add_argument("--source-file", default="data/products.csv")
    parser.add_argument("--bronze-base-path", default=BRONZE_BASE_PATH)
    parser.add_argument("--table-name", default="bronze_products")
    args = parser.parse_args()

    spark, owns_session = get_or_create_spark("bronze-ingest-products")
    try:
        ingest_products(
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
