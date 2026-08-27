"""Apply Silver completeness and logic/type checks to Products.

AI rationale: product IDs and numeric inventory values are flagged in place;
no product rows are filtered, corrected, or deduplicated.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

try:
    from ._common import (
        BRONZE_PRODUCTS_TABLE,
        SILVER_PRODUCTS_TABLE,
        CheckDefinition,
        SilverResult,
        add_quality_result,
        boolean_check,
        build_quality_metrics,
        get_or_create_spark,
        is_missing,
        log_silver_complete,
        log_silver_start,
        read_bronze_table,
        write_silver_table,
    )
except ImportError:
    from _common import (  # type: ignore[no-redef]
        BRONZE_PRODUCTS_TABLE,
        SILVER_PRODUCTS_TABLE,
        CheckDefinition,
        SilverResult,
        add_quality_result,
        boolean_check,
        build_quality_metrics,
        get_or_create_spark,
        is_missing,
        log_silver_complete,
        log_silver_start,
        read_bronze_table,
        write_silver_table,
    )

PRODUCTS_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DecimalType(10, 2), True),
        StructField("cost", DecimalType(10, 2), True),
        StructField("stock_quantity", IntegerType(), True),
        StructField("reorder_level", IntegerType(), True),
    ]
)

PRODUCT_CHECKS = [
    CheckDefinition(
        "COMPLETENESS",
        "dq_completeness_pass",
        "FAILED_COMPLETENESS",
        "COMP_NULL_PRODUCT_ID",
    ),
    CheckDefinition(
        "LOGIC_TYPE",
        "dq_logic_type_pass",
        "FAILED_LOGIC_TYPE",
        "LOGIC_INVALID_PRODUCT_VALUE",
    ),
]


def build_silver_products(bronze_products: DataFrame) -> DataFrame:
    """Flag product defects while preserving every Bronze row."""
    checked = (
        bronze_products.withColumn(
            "dq_completeness_pass",
            boolean_check(~is_missing("product_id")),
        )
        .withColumn(
            "dq_logic_type_pass",
            boolean_check(
                F.col("price").isNotNull()
                & (F.col("price") > 0)
                & F.col("cost").isNotNull()
                & (F.col("cost") > 0)
                & F.col("stock_quantity").isNotNull()
                & (F.col("stock_quantity") >= 0)
                & F.col("reorder_level").isNotNull()
                & (F.col("reorder_level") >= 0)
            ),
        )
    )
    return add_quality_result(checked, PRODUCT_CHECKS)


def transform_products(
    spark: SparkSession,
    source_table: str = BRONZE_PRODUCTS_TABLE,
    target_table: str = SILVER_PRODUCTS_TABLE,
) -> SilverResult:
    """Read, flag, measure, and overwrite the managed Silver Products table."""
    started_at, timer_started = log_silver_start(
        "products", source_table, target_table
    )
    bronze = read_bronze_table(spark, source_table, PRODUCTS_SCHEMA)
    source_count = bronze.count()
    silver = build_silver_products(bronze)
    metrics = build_quality_metrics(silver, "products", PRODUCT_CHECKS)
    target_count = write_silver_table(silver, source_count, target_table)
    completed_at, duration = log_silver_complete(
        "products", source_count, target_count, timer_started
    )
    return SilverResult(
        dataset="products",
        source_table=source_table,
        target_table=target_table,
        source_count=source_count,
        target_count=target_count,
        metrics=metrics,
        started_at=started_at,
        completed_at=completed_at,
        duration_seconds=duration,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Transform Bronze Products to Silver.")
    parser.add_argument("--source-table", default=BRONZE_PRODUCTS_TABLE)
    parser.add_argument("--target-table", default=SILVER_PRODUCTS_TABLE)
    args, _ = parser.parse_known_args()

    spark, owns_session = get_or_create_spark("silver-transform-products")
    try:
        transform_products(spark, args.source_table, args.target_table)
    finally:
        if owns_session:
            spark.stop()


if __name__ == "__main__":
    main()
