"""Run all Bronze ingestions in dependency-friendly order.

AI rationale: orchestrate the three reusable dataset functions directly with
one Spark session; no external scheduler is needed for this assessment.
Validation remains inside each ingestion function and stops orchestration on
the first failed source or target assertion.
"""

from __future__ import annotations

import argparse
import importlib
from typing import Callable

from pyspark.sql import SparkSession

try:
    from ._common import (
        BRONZE_BASE_PATH,
        IngestionResult,
        get_or_create_spark,
        join_path,
    )
except ImportError:
    from _common import (  # type: ignore[no-redef]
        BRONZE_BASE_PATH,
        IngestionResult,
        get_or_create_spark,
        join_path,
    )

IngestionFunction = Callable[
    [SparkSession, str, str, str],
    IngestionResult,
]


def _load_ingestion_function(
    module_name: str, function_name: str
) -> IngestionFunction:
    """Load numbered phase modules, which cannot use normal import syntax."""
    qualified_name = (
        f"{__package__}.{module_name}" if __package__ else module_name
    )
    module = importlib.import_module(qualified_name)
    return getattr(module, function_name)


def ingest_all(
    spark: SparkSession,
    input_base_path: str = "data",
    bronze_base_path: str = BRONZE_BASE_PATH,
) -> list[IngestionResult]:
    """Ingest Customers, Products, then Orders using one Spark session."""
    ingest_customers = _load_ingestion_function(
        "01_ingest_customers", "ingest_customers"
    )
    ingest_products = _load_ingestion_function(
        "03_ingest_products", "ingest_products"
    )
    ingest_orders = _load_ingestion_function("02_ingest_orders", "ingest_orders")

    results = [
        ingest_customers(
            spark,
            join_path(input_base_path, "customers.csv"),
            bronze_base_path,
            "bronze_customers",
        ),
        ingest_products(
            spark,
            join_path(input_base_path, "products.csv"),
            bronze_base_path,
            "bronze_products",
        ),
        ingest_orders(
            spark,
            join_path(input_base_path, "orders.csv"),
            bronze_base_path,
            "bronze_orders",
        ),
    ]
    print(
        "[BRONZE PIPELINE COMPLETE] "
        + " | ".join(
            f"{result.target_table}={result.target_count_after:,}"
            for result in results
        )
    )
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Run all Bronze ingestions.")
    parser.add_argument("--input-base-path", default="data")
    parser.add_argument("--bronze-base-path", default=BRONZE_BASE_PATH)
    args = parser.parse_args()

    spark, owns_session = get_or_create_spark("bronze-ingest-all")
    try:
        ingest_all(
            spark,
            input_base_path=args.input_base_path,
            bronze_base_path=args.bronze_base_path,
        )
    finally:
        if owns_session:
            spark.stop()


if __name__ == "__main__":
    main()
