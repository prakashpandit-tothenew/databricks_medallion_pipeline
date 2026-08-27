"""Apply Silver completeness, uniqueness, and logic checks to Customers.

AI rationale: all Bronze rows remain in Silver; duplicate customer IDs are
identified with a window and flagged rather than removed.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)

try:
    from ._common import (
        BRONZE_CUSTOMERS_TABLE,
        SILVER_CUSTOMERS_TABLE,
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
        BRONZE_CUSTOMERS_TABLE,
        SILVER_CUSTOMERS_TABLE,
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

CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("signup_date", DateType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("lifetime_value", DecimalType(12, 2), True),
    ]
)

CUSTOMER_CHECKS = [
    CheckDefinition(
        "COMPLETENESS",
        "dq_completeness_pass",
        "FAILED_COMPLETENESS",
        "COMP_CRITICAL_NULL",
    ),
    CheckDefinition(
        "UNIQUENESS",
        "dq_uniqueness_pass",
        "FAILED_UNIQUENESS",
        "UNIQ_DUPLICATE_CUSTOMER_ID",
    ),
    CheckDefinition(
        "LOGIC_TYPE",
        "dq_logic_type_pass",
        "FAILED_LOGIC_TYPE",
        "LOGIC_INVALID_CUSTOMER_VALUE",
    ),
]


def build_silver_customers(bronze_customers: DataFrame) -> DataFrame:
    """Flag customer defects without dropping or correcting source rows."""
    duplicate_window = Window.partitionBy("customer_id")
    with_counts = bronze_customers.withColumn(
        "_customer_id_count",
        F.count(F.lit(1)).over(duplicate_window),
    )

    checked = (
        with_counts.withColumn(
            "dq_completeness_pass",
            boolean_check(~is_missing("customer_id") & ~is_missing("email")),
        )
        .withColumn(
            "dq_uniqueness_pass",
            boolean_check(
                F.when(is_missing("customer_id"), F.lit(True)).otherwise(
                    F.col("_customer_id_count") == 1
                )
            ),
        )
        .withColumn(
            "dq_logic_type_pass",
            boolean_check(
                F.col("signup_date").isNotNull()
                & (F.col("signup_date") <= F.current_date())
                & F.col("lifetime_value").isNotNull()
                & (F.col("lifetime_value") >= 0)
            ),
        )
        .drop("_customer_id_count")
    )
    return add_quality_result(checked, CUSTOMER_CHECKS)


def transform_customers(
    spark: SparkSession,
    source_table: str = BRONZE_CUSTOMERS_TABLE,
    target_table: str = SILVER_CUSTOMERS_TABLE,
) -> SilverResult:
    """Read, flag, measure, and overwrite the managed Silver Customers table."""
    started_at, timer_started = log_silver_start(
        "customers", source_table, target_table
    )
    bronze = read_bronze_table(spark, source_table, CUSTOMERS_SCHEMA)
    source_count = bronze.count()
    silver = build_silver_customers(bronze)
    metrics = build_quality_metrics(silver, "customers", CUSTOMER_CHECKS)
    target_count = write_silver_table(silver, source_count, target_table)
    completed_at, duration = log_silver_complete(
        "customers", source_count, target_count, timer_started
    )
    return SilverResult(
        dataset="customers",
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
    parser = argparse.ArgumentParser(description="Transform Bronze Customers to Silver.")
    parser.add_argument("--source-table", default=BRONZE_CUSTOMERS_TABLE)
    parser.add_argument("--target-table", default=SILVER_CUSTOMERS_TABLE)
    args, _ = parser.parse_known_args()

    spark, owns_session = get_or_create_spark("silver-transform-customers")
    try:
        transform_customers(spark, args.source_table, args.target_table)
    finally:
        if owns_session:
            spark.stop()


if __name__ == "__main__":
    main()
