"""Run Customers, Products, Orders, then publish Silver quality metrics.

AI rationale: use one Spark session and one final metrics write. Parent datasets
run before Orders so referential checks use the same Bronze snapshot.
"""

from __future__ import annotations

import argparse
import importlib
from typing import Callable

from pyspark.sql import SparkSession

try:
    from ._common import (
        QUALITY_SUMMARY_TABLE,
        SilverResult,
        get_or_create_spark,
        write_quality_summary,
    )
except ImportError:
    from _common import (  # type: ignore[no-redef]
        QUALITY_SUMMARY_TABLE,
        SilverResult,
        get_or_create_spark,
        write_quality_summary,
    )

TransformFunction = Callable[..., SilverResult]


def _load_transform(module_name: str, function_name: str) -> TransformFunction:
    """Load numbered phase modules, which cannot use normal import statements."""
    qualified_name = (
        f"{__package__}.{module_name}" if __package__ else module_name
    )
    return getattr(importlib.import_module(qualified_name), function_name)


def transform_all(
    spark: SparkSession,
    quality_summary_table: str = QUALITY_SUMMARY_TABLE,
) -> list[SilverResult]:
    """Transform all entities and replace the combined quality summary."""
    transform_customers = _load_transform(
        "01_transform_customers", "transform_customers"
    )
    transform_products = _load_transform(
        "02_transform_products", "transform_products"
    )
    transform_orders = _load_transform("03_transform_orders", "transform_orders")

    results = [
        transform_customers(spark),
        transform_products(spark),
        transform_orders(spark),
    ]
    metrics = results[0].metrics
    for result in results[1:]:
        metrics = metrics.unionByName(result.metrics)

    metric_count = write_quality_summary(metrics, quality_summary_table)
    print(
        f"[SILVER QUALITY SUMMARY] table={quality_summary_table} | "
        f"metric_rows={metric_count}"
    )
    metrics.orderBy("dataset", "check_name").show(truncate=False)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Run all Silver transformations.")
    parser.add_argument("--quality-summary-table", default=QUALITY_SUMMARY_TABLE)
    args, _ = parser.parse_known_args()

    spark, owns_session = get_or_create_spark("silver-transform-all")
    try:
        transform_all(spark, args.quality_summary_table)
    finally:
        if owns_session:
            spark.stop()


if __name__ == "__main__":
    main()
