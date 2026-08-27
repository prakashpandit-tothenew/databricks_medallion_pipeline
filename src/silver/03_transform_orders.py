"""Apply all four required Silver data-quality checks to Orders.

AI rationale:
    Use a window for duplicate order IDs and left anti-joins for orphan keys.
    Join only orphan markers back to Orders, so all valid and invalid rows are
    retained and receive deterministic quality flags.
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
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
        BRONZE_CUSTOMERS_TABLE,
        BRONZE_ORDERS_TABLE,
        BRONZE_PRODUCTS_TABLE,
        SILVER_ORDERS_TABLE,
        CheckDefinition,
        SilverResult,
        add_quality_result,
        boolean_check,
        build_quality_metrics,
        get_or_create_spark,
        is_missing,
        log_silver_complete,
        log_silver_start,
        qualify_table,
        read_bronze_table,
        write_silver_table,
    )
except ImportError:
    from _common import (  # type: ignore[no-redef]
        BRONZE_CUSTOMERS_TABLE,
        BRONZE_ORDERS_TABLE,
        BRONZE_PRODUCTS_TABLE,
        SILVER_ORDERS_TABLE,
        CheckDefinition,
        SilverResult,
        add_quality_result,
        boolean_check,
        build_quality_metrics,
        get_or_create_spark,
        is_missing,
        log_silver_complete,
        log_silver_start,
        qualify_table,
        read_bronze_table,
        write_silver_table,
    )

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_date", DateType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DecimalType(10, 2), True),
        StructField("total_amount", DecimalType(12, 2), True),
        StructField("order_status", StringType(), True),
        StructField("payment_date", DateType(), True),
    ]
)

ORDER_CHECKS = [
    CheckDefinition(
        "COMPLETENESS",
        "dq_completeness_pass",
        "FAILED_COMPLETENESS",
        "COMP_NULL_ORDER_REFERENCE",
    ),
    CheckDefinition(
        "UNIQUENESS",
        "dq_uniqueness_pass",
        "FAILED_UNIQUENESS",
        "UNIQ_DUPLICATE_ORDER_ID",
    ),
    CheckDefinition(
        "REFERENTIAL_INTEGRITY",
        "dq_referential_integrity_pass",
        "FAILED_REFERENTIAL_INTEGRITY",
        "REF_MISSING_CUSTOMER_OR_PRODUCT",
    ),
    CheckDefinition(
        "LOGIC_TYPE",
        "dq_logic_type_pass",
        "FAILED_LOGIC_TYPE",
        "LOGIC_INVALID_ORDER_VALUE",
    ),
]


def _orphan_keys(
    child: DataFrame,
    child_key: str,
    parent: DataFrame,
    parent_key: str,
    marker_column: str,
) -> DataFrame:
    """Find non-NULL child keys with no parent using the required left anti-join."""
    child_keys = (
        child.select(child_key)
        .where(~is_missing(child_key))
        .distinct()
    )
    parent_keys = (
        parent.select(F.col(parent_key).alias(child_key))
        .where(~is_missing(child_key))
        .distinct()
    )
    return child_keys.join(parent_keys, child_key, "left_anti").withColumn(
        marker_column, F.lit(True)
    )


def build_silver_orders(
    bronze_orders: DataFrame,
    customer_keys: DataFrame,
    product_keys: DataFrame,
) -> DataFrame:
    """Flag completeness, duplicate, orphan, date, and numeric failures."""
    orphan_customers = _orphan_keys(
        bronze_orders,
        "customer_id",
        customer_keys,
        "customer_id",
        "_orphan_customer",
    )
    orphan_products = _orphan_keys(
        bronze_orders,
        "product_id",
        product_keys,
        "product_id",
        "_orphan_product",
    )
    order_window = Window.partitionBy("order_id")
    source_columns = bronze_orders.columns

    checked = (
        bronze_orders.join(orphan_customers, "customer_id", "left")
        .join(orphan_products, "product_id", "left")
        .select(
            *source_columns,
            "_orphan_customer",
            "_orphan_product",
        )
        .withColumn("_order_id_count", F.count(F.lit(1)).over(order_window))
        .withColumn(
            "dq_completeness_pass",
            boolean_check(
                ~is_missing("customer_id") & ~is_missing("product_id")
            ),
        )
        .withColumn(
            "dq_uniqueness_pass",
            boolean_check(
                F.when(is_missing("order_id"), F.lit(True)).otherwise(
                    F.col("_order_id_count") == 1
                )
            ),
        )
        .withColumn(
            "dq_referential_integrity_pass",
            boolean_check(
                F.col("_orphan_customer").isNull()
                & F.col("_orphan_product").isNull()
            ),
        )
        .withColumn(
            "dq_logic_type_pass",
            boolean_check(
                F.col("order_date").isNotNull()
                & (F.col("order_date") <= F.current_date())
                & F.col("payment_date").isNotNull()
                & (F.col("payment_date") >= F.col("order_date"))
                & F.col("quantity").isNotNull()
                & (F.col("quantity") > 0)
                & F.col("unit_price").isNotNull()
                & (F.col("unit_price") > 0)
                & F.col("total_amount").isNotNull()
                & (F.col("total_amount") > 0)
                & (
                    F.abs(
                        F.col("total_amount")
                        - (F.col("quantity") * F.col("unit_price"))
                    )
                    <= F.lit(0.01)
                )
            ),
        )
        .drop("_orphan_customer", "_orphan_product", "_order_id_count")
    )
    return add_quality_result(checked, ORDER_CHECKS)


def transform_orders(
    spark: SparkSession,
    source_table: str = BRONZE_ORDERS_TABLE,
    customer_table: str = BRONZE_CUSTOMERS_TABLE,
    product_table: str = BRONZE_PRODUCTS_TABLE,
    target_table: str = SILVER_ORDERS_TABLE,
) -> SilverResult:
    """Read, flag, measure, and overwrite the managed Silver Orders table."""
    started_at, timer_started = log_silver_start("orders", source_table, target_table)
    bronze_orders = read_bronze_table(spark, source_table, ORDERS_SCHEMA)
    customer_keys = spark.table(qualify_table(customer_table)).select("customer_id")
    product_keys = spark.table(qualify_table(product_table)).select("product_id")
    source_count = bronze_orders.count()
    silver = build_silver_orders(bronze_orders, customer_keys, product_keys)
    metrics = build_quality_metrics(silver, "orders", ORDER_CHECKS)
    target_count = write_silver_table(silver, source_count, target_table)
    completed_at, duration = log_silver_complete(
        "orders", source_count, target_count, timer_started
    )
    return SilverResult(
        dataset="orders",
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
    parser = argparse.ArgumentParser(description="Transform Bronze Orders to Silver.")
    parser.add_argument("--source-table", default=BRONZE_ORDERS_TABLE)
    parser.add_argument("--customer-table", default=BRONZE_CUSTOMERS_TABLE)
    parser.add_argument("--product-table", default=BRONZE_PRODUCTS_TABLE)
    parser.add_argument("--target-table", default=SILVER_ORDERS_TABLE)
    args, _ = parser.parse_known_args()

    spark, owns_session = get_or_create_spark("silver-transform-orders")
    try:
        transform_orders(
            spark,
            args.source_table,
            args.customer_table,
            args.product_table,
            args.target_table,
        )
    finally:
        if owns_session:
            spark.stop()


if __name__ == "__main__":
    main()
