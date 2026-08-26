"""Shared Bronze CSV-to-Delta ingestion utilities.

AI rationale:
    Centralize only technical Bronze behavior (source/header validation,
    metadata, append-only Delta writes, and logging). Dataset schemas remain in
    the individual ingestion scripts so each source contract is explicit.

Validation:
    Verify the source exists, source columns and counts match the contract,
    metadata columns are present, and the post-write table count increased by
    exactly the source count. No Silver-level data-quality rules run here.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from time import perf_counter
from typing import Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

BRONZE_BASE_PATH = "/Volumes/poc_catalog/default/poc_volume/bronze"
METADATA_COLUMNS = ["_ingestion_timestamp", "_source_file"]
_TABLE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class IngestionResult:
    """Counts and destinations produced by one successful Bronze ingestion."""

    source_file: str
    source_count: int
    target_table: str
    target_count_before: int
    target_count_after: int
    target_path: str
    started_at: datetime
    completed_at: datetime
    duration_seconds: float


def join_path(base_path: str, child: str) -> str:
    """Join repository-relative or DBFS-style paths without OS assumptions."""
    return f"{base_path.rstrip('/')}/{child.lstrip('/')}"


def source_exists(spark: SparkSession, source_file: str) -> bool:
    """Check local, DBFS, or Volume paths without JVM-only Spark APIs."""
    try:
        # A one-line text read works with classic Spark and Spark Connect,
        # including Databricks serverless paths under /Volumes.
        return spark.read.text(source_file).limit(1).count() > 0
    except Exception as error:
        raise FileNotFoundError(
            f"Bronze source file is missing or inaccessible: {source_file}"
        ) from error


def read_source_csv(
    spark: SparkSession,
    source_file: str,
    schema: StructType,
) -> DataFrame:
    """Read one CSV with an explicit schema while retaining raw defect rows."""
    if not source_exists(spark, source_file):
        raise FileNotFoundError(f"Bronze source file does not exist: {source_file}")

    return (
        spark.read.schema(schema)
        .option("header", "true")
        .option("enforceSchema", "false")
        .option("mode", "PERMISSIVE")
        .option("nullValue", "")
        .option("emptyValue", "")
        .option("dateFormat", "yyyy-MM-dd")
        .option("encoding", "UTF-8")
        .csv(source_file)
    )


def validate_source_dataframe(
    dataframe: DataFrame,
    expected_columns: Sequence[str],
    expected_count: int,
) -> int:
    """Perform ingestion-only schema and row-count validation."""
    actual_columns = dataframe.columns
    if actual_columns != list(expected_columns):
        raise ValueError(
            "Source columns do not match contract. "
            f"Expected {list(expected_columns)}, got {actual_columns}"
        )

    source_count = dataframe.count()
    if source_count != expected_count:
        raise ValueError(
            f"Source row count mismatch: expected {expected_count:,}, "
            f"got {source_count:,}"
        )
    return source_count


def add_bronze_metadata(dataframe: DataFrame) -> DataFrame:
    """Add lineage columns without modifying any source column."""
    source_columns = dataframe.columns
    bronze_dataframe = (
        dataframe.withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )
    if bronze_dataframe.columns != [*source_columns, *METADATA_COLUMNS]:
        raise ValueError("Bronze metadata columns were not added as expected")
    return bronze_dataframe


def _validate_table_name(table_name: str) -> None:
    if not _TABLE_NAME_PATTERN.fullmatch(table_name):
        raise ValueError(
            f"Invalid Hive-metastore table name {table_name!r}; "
            "use letters, numbers, and underscores only"
        )


def _table_count(spark: SparkSession, table_name: str) -> int:
    if not spark.catalog.tableExists(table_name):
        return 0
    return spark.table(table_name).count()


def write_append_delta(
    spark: SparkSession,
    dataframe: DataFrame,
    table_name: str,
    target_path: str,
    source_count: int,
    source_columns: Sequence[str],
) -> Tuple[int, int]:
    """Append to a path-backed Hive-metastore Delta table and validate counts."""
    _validate_table_name(table_name)
    target_count_before = _table_count(spark, table_name)

    (
        dataframe.write.format("delta")
        .mode("append")
        .option("path", target_path)
        .saveAsTable(table_name)
    )

    target_dataframe = spark.table(table_name)
    expected_columns = [*source_columns, *METADATA_COLUMNS]
    if target_dataframe.columns != expected_columns:
        raise ValueError(
            f"Bronze table {table_name} columns do not match: "
            f"expected {expected_columns}, got {target_dataframe.columns}"
        )

    target_count_after = target_dataframe.count()
    expected_target_count = target_count_before + source_count
    if target_count_after != expected_target_count:
        raise ValueError(
            f"Bronze table {table_name} count mismatch after append: "
            f"expected {expected_target_count:,}, got {target_count_after:,}"
        )
    return target_count_before, target_count_after


def ingest_csv_to_delta(
    spark: SparkSession,
    *,
    source_file: str,
    schema: StructType,
    expected_columns: Sequence[str],
    expected_count: int,
    table_name: str,
    target_path: str,
) -> IngestionResult:
    """Run and log one append-only Bronze ingestion."""
    started_at = datetime.now(timezone.utc)
    timer_started = perf_counter()
    print(
        f"[BRONZE START] {started_at.isoformat()} | "
        f"source={source_file} | target={table_name}"
    )

    try:
        source_dataframe = read_source_csv(spark, source_file, schema)
        source_count = validate_source_dataframe(
            source_dataframe, expected_columns, expected_count
        )
        print(f"[BRONZE SOURCE] file={source_file} | rows={source_count:,}")

        bronze_dataframe = add_bronze_metadata(source_dataframe)
        target_count_before, target_count_after = write_append_delta(
            spark=spark,
            dataframe=bronze_dataframe,
            table_name=table_name,
            target_path=target_path,
            source_count=source_count,
            source_columns=expected_columns,
        )
    except Exception as error:
        duration = perf_counter() - timer_started
        print(
            f"[BRONZE FAILED] source={source_file} | target={table_name} | "
            f"duration_seconds={duration:.2f} | "
            f"error={type(error).__name__}: {error}"
        )
        raise

    completed_at = datetime.now(timezone.utc)
    duration = perf_counter() - timer_started
    print(
        f"[BRONZE COMPLETE] {completed_at.isoformat()} | "
        f"target={table_name} | before={target_count_before:,} | "
        f"appended={source_count:,} | after={target_count_after:,} | "
        f"duration_seconds={duration:.2f}"
    )
    return IngestionResult(
        source_file=source_file,
        source_count=source_count,
        target_table=table_name,
        target_count_before=target_count_before,
        target_count_after=target_count_after,
        target_path=target_path,
        started_at=started_at,
        completed_at=completed_at,
        duration_seconds=duration,
    )


def get_or_create_spark(app_name: str) -> Tuple[SparkSession, bool]:
    """Reuse Databricks' session or create one when launched by spark-submit."""
    active_session = SparkSession.getActiveSession()
    if active_session is not None:
        return active_session, False
    return SparkSession.builder.appName(app_name).getOrCreate(), True
