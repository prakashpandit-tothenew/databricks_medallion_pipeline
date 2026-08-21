"""Append raw Customers CSV rows to the Bronze Delta table.

AI rationale: keep the source contract explicit in this dataset entrypoint and
delegate only shared technical ingestion behavior to ``_common``.
Validation: require 10,000 source rows and the exact seven source columns; no
customer quality checks, corrections, or deduplication occur in Bronze.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    DecimalType,
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

CUSTOMERS_SCHEMA = StructType(
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
CUSTOMERS_COLUMNS = [
    "customer_id",
    "customer_name",
    "email",
    "country",
    "signup_date",
    "customer_segment",
    "lifetime_value",
]
EXPECTED_CUSTOMER_COUNT = 10_000


def ingest_customers(
    spark: SparkSession,
    source_file: str = "data/customers.csv",
    bronze_base_path: str = BRONZE_BASE_PATH,
    table_name: str = "bronze_customers",
) -> IngestionResult:
    """Ingest Customers using the explicit source schema and append-only Delta."""
    return ingest_csv_to_delta(
        spark=spark,
        source_file=source_file,
        schema=CUSTOMERS_SCHEMA,
        expected_columns=CUSTOMERS_COLUMNS,
        expected_count=EXPECTED_CUSTOMER_COUNT,
        table_name=table_name,
        target_path=join_path(bronze_base_path, "customers"),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest Customers into Bronze Delta.")
    parser.add_argument("--source-file", default="data/customers.csv")
    parser.add_argument("--bronze-base-path", default=BRONZE_BASE_PATH)
    parser.add_argument("--table-name", default="bronze_customers")
    args = parser.parse_args()

    spark, owns_session = get_or_create_spark("bronze-ingest-customers")
    try:
        ingest_customers(
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
