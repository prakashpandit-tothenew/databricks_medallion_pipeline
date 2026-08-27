"""Shared Silver data-quality, metrics, and managed-Delta utilities.

AI rationale:
    Keep each dataset's business rules in its own script while centralizing
    schema validation, quality-result packing, metrics, full-row preservation,
    and Unity Catalog managed-table writes.

Validation:
    Silver writes must preserve the Bronze row count. Every configured check
    produces a Boolean pass column, a quality status, and summary metrics.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from time import perf_counter
from typing import Sequence, Tuple

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

TABLE_SCHEMA = "poc_catalog.default"
BRONZE_CUSTOMERS_TABLE = f"{TABLE_SCHEMA}.bronze_customers"
BRONZE_PRODUCTS_TABLE = f"{TABLE_SCHEMA}.bronze_products"
BRONZE_ORDERS_TABLE = f"{TABLE_SCHEMA}.bronze_orders"
SILVER_CUSTOMERS_TABLE = f"{TABLE_SCHEMA}.silver_customers"
SILVER_PRODUCTS_TABLE = f"{TABLE_SCHEMA}.silver_products"
SILVER_ORDERS_TABLE = f"{TABLE_SCHEMA}.silver_orders"
QUALITY_SUMMARY_TABLE = f"{TABLE_SCHEMA}.silver_quality_summary"

QUALITY_RESULT_COLUMN = "quality_check_result"
QUALITY_CODES_COLUMN = "quality_error_codes"


@dataclass(frozen=True)
class CheckDefinition:
    """One row-level quality check and its stable failure labels."""

    name: str
    pass_column: str
    failure_status: str
    error_code: str


@dataclass(frozen=True)
class SilverResult:
    """Output and metrics from one successful Silver transformation."""

    dataset: str
    source_table: str
    target_table: str
    source_count: int
    target_count: int
    metrics: DataFrame
    started_at: datetime
    completed_at: datetime
    duration_seconds: float


def qualify_table(table_name: str) -> str:
    """Use a supplied three-part name or the assessment's UC schema."""
    return table_name if "." in table_name else f"{TABLE_SCHEMA}.{table_name}"


def read_bronze_table(
    spark: SparkSession,
    table_name: str,
    expected_schema: StructType,
    metadata_columns: Sequence[str] = ("_ingestion_timestamp", "_source_file"),
) -> DataFrame:
    """Read Bronze and enforce source names/types without schema inference."""
    qualified_name = qualify_table(table_name)
    dataframe = spark.table(qualified_name)
    expected_columns = [field.name for field in expected_schema.fields]
    required_columns = [*expected_columns, *metadata_columns]
    missing_columns = [
        column_name
        for column_name in required_columns
        if column_name not in dataframe.columns
    ]
    if missing_columns:
        raise ValueError(
            f"Bronze table {qualified_name} is missing columns: {missing_columns}"
        )

    actual_types = {
        field.name: field.dataType.simpleString()
        for field in dataframe.schema.fields
        if field.name in expected_columns
    }
    expected_types = {
        field.name: field.dataType.simpleString()
        for field in expected_schema.fields
    }
    if actual_types != expected_types:
        raise ValueError(
            f"Bronze table {qualified_name} type mismatch: "
            f"expected {expected_types}, got {actual_types}"
        )
    return dataframe


def is_missing(column_name: str) -> Column:
    """Treat SQL NULL and blank strings as incomplete without changing values."""
    column = F.col(column_name)
    return column.isNull() | (F.trim(column.cast("string")) == "")


def boolean_check(condition: Column) -> Column:
    """Convert SQL three-valued conditions into a strict pass/fail Boolean."""
    return F.coalesce(condition.cast("boolean"), F.lit(False))


def add_quality_result(
    dataframe: DataFrame,
    checks: Sequence[CheckDefinition],
) -> DataFrame:
    """Add stable status and error-code strings while retaining every row."""
    if not checks:
        raise ValueError("At least one Silver quality check is required")

    all_passed = F.lit(True)
    status_values = []
    error_code_values = []
    for check in checks:
        passed = boolean_check(F.col(check.pass_column))
        all_passed = all_passed & passed
        status_values.append(
            F.when(~passed, F.lit(check.failure_status))
        )
        error_code_values.append(
            F.when(~passed, F.lit(check.error_code))
        )

    return (
        dataframe.withColumn(
            QUALITY_RESULT_COLUMN,
            F.when(all_passed, F.lit("PASSED")).otherwise(
                F.concat_ws(";", *status_values)
            ),
        )
        .withColumn(
            QUALITY_CODES_COLUMN,
            F.when(all_passed, F.lit("")).otherwise(
                F.concat_ws(";", *error_code_values)
            ),
        )
    )


def build_quality_metrics(
    dataframe: DataFrame,
    dataset: str,
    checks: Sequence[CheckDefinition],
) -> DataFrame:
    """Return total/passed/failed/pass-percentage metrics for each check."""
    metric_frames = []
    for check in checks:
        passed = boolean_check(F.col(check.pass_column))
        metric_frames.append(
            dataframe.agg(
                F.count(F.lit(1)).cast("long").alias("total_row_count"),
                F.sum(F.when(passed, 1).otherwise(0))
                .cast("long")
                .alias("passed_count"),
                F.sum(F.when(~passed, 1).otherwise(0))
                .cast("long")
                .alias("failed_count"),
            )
            .withColumn("dataset", F.lit(dataset))
            .withColumn("check_name", F.lit(check.name))
            .withColumn(
                "pass_percentage",
                F.when(
                    F.col("total_row_count") == 0,
                    F.lit(100.0),
                ).otherwise(
                    F.round(
                        F.col("passed_count") * F.lit(100.0)
                        / F.col("total_row_count"),
                        2,
                    )
                ),
            )
            .withColumn("measured_at", F.current_timestamp())
            .select(
                "dataset",
                "check_name",
                "total_row_count",
                "passed_count",
                "failed_count",
                "pass_percentage",
                "measured_at",
            )
        )

    metrics = metric_frames[0]
    for metric_frame in metric_frames[1:]:
        metrics = metrics.unionByName(metric_frame)
    return metrics


def write_silver_table(
    dataframe: DataFrame,
    source_count: int,
    target_table: str,
) -> int:
    """Replace one managed Silver table and assert no Bronze rows were lost."""
    qualified_name = qualify_table(target_table)
    (
        dataframe.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(qualified_name)
    )
    target_count = dataframe.sparkSession.table(qualified_name).count()
    if target_count != source_count:
        raise ValueError(
            f"Silver row preservation failed for {qualified_name}: "
            f"source={source_count:,}, target={target_count:,}"
        )
    return target_count


def write_quality_summary(
    metrics: DataFrame,
    target_table: str = QUALITY_SUMMARY_TABLE,
) -> int:
    """Replace the managed per-check metrics table for the current run."""
    qualified_name = qualify_table(target_table)
    (
        metrics.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(qualified_name)
    )
    return metrics.sparkSession.table(qualified_name).count()


def log_silver_start(dataset: str, source_table: str, target_table: str) -> Tuple[datetime, float]:
    """Log start and return timestamps used by dataset transforms."""
    started_at = datetime.now(timezone.utc)
    timer_started = perf_counter()
    print(
        f"[SILVER START] {started_at.isoformat()} | dataset={dataset} | "
        f"source={qualify_table(source_table)} | target={qualify_table(target_table)}"
    )
    return started_at, timer_started


def log_silver_complete(
    dataset: str,
    source_count: int,
    target_count: int,
    timer_started: float,
) -> Tuple[datetime, float]:
    """Log completion counts and duration."""
    completed_at = datetime.now(timezone.utc)
    duration = perf_counter() - timer_started
    print(
        f"[SILVER COMPLETE] {completed_at.isoformat()} | dataset={dataset} | "
        f"source_rows={source_count:,} | target_rows={target_count:,} | "
        f"duration_seconds={duration:.2f}"
    )
    return completed_at, duration


def get_or_create_spark(app_name: str) -> Tuple[SparkSession, bool]:
    """Reuse the Databricks session or create one under spark-submit."""
    active_session = SparkSession.getActiveSession()
    if active_session is not None:
        return active_session, False
    return SparkSession.builder.appName(app_name).getOrCreate(), True
