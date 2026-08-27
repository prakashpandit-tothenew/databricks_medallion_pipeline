"""Execute Gold SQL aggregations and persist managed Delta tables.

AI rationale:
    Keep business logic in Spark SQL files so Databricks SQL and PySpark share
    one definition. This runner only loads those files, executes them, and logs
    table row counts. Gold uses overwrite-style CREATE OR REPLACE (idempotent);
    Bronze remains append-only.

Validation:
    Each SQL file must produce its managed table. After execution the runner
    asserts the table exists and logs row counts. Measures come from Silver
    rows where quality_check_result = 'PASSED'; defective rows stay in Silver.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from typing import Sequence, Tuple

from pyspark.sql import SparkSession

TABLE_SCHEMA = "poc_catalog.default"

SILVER_TABLES: Sequence[str] = (
    f"{TABLE_SCHEMA}.silver_customers",
    f"{TABLE_SCHEMA}.silver_products",
    f"{TABLE_SCHEMA}.silver_orders",
)

GOLD_JOBS: Sequence[Tuple[str, str]] = (
    ("01_sales_by_product.sql", f"{TABLE_SCHEMA}.gold_sales_by_product"),
    ("02_revenue_by_customer.sql", f"{TABLE_SCHEMA}.gold_revenue_by_customer"),
    ("03_daily_weekly_trends.sql", f"{TABLE_SCHEMA}.gold_daily_weekly_trends"),
    ("04_customer_segmentation.sql", f"{TABLE_SCHEMA}.gold_customer_segmentation"),
)


def _table_exists(spark: SparkSession, table_name: str) -> bool:
    try:
        spark.table(table_name).limit(0)
        return True
    except Exception:
        return False


def assert_silver_ready(spark: SparkSession) -> None:
    """Fail fast when Silver is missing, empty, or has no PASSED rows."""
    print("[GOLD PREFLIGHT] Silver source counts")
    passed_by_table = {}
    for table_name in SILVER_TABLES:
        if not _table_exists(spark, table_name):
            raise FileNotFoundError(
                f"Silver table {table_name} does not exist. "
                "Run src/silver/transform_all.py before Gold."
            )
        total = spark.table(table_name).count()
        passed = spark.sql(
            f"""
            SELECT count(*) AS passed_count
            FROM {table_name}
            WHERE upper(trim(coalesce(quality_check_result, ''))) IN ('PASSED', 'PASS')
            """
        ).collect()[0]["passed_count"]
        statuses = [
            row["quality_check_result"]
            for row in spark.sql(
                f"""
                SELECT quality_check_result, count(*) AS row_count
                FROM {table_name}
                GROUP BY quality_check_result
                ORDER BY row_count DESC
                """
            ).collect()
        ]
        passed_by_table[table_name] = passed
        print(
            f"[GOLD PREFLIGHT] {table_name} | total={total:,} | "
            f"passed={passed:,} | quality_check_result={statuses}"
        )
        if total == 0:
            raise ValueError(
                f"{table_name} is empty. Re-run Silver (transform_all) on "
                "populated Bronze tables before Gold."
            )

    if passed_by_table[f"{TABLE_SCHEMA}.silver_customers"] == 0:
        raise ValueError(
            "No Silver customers have quality_check_result PASSED/PASS. "
            "Inspect poc_catalog.default.silver_customers before Gold."
        )
    if passed_by_table[f"{TABLE_SCHEMA}.silver_orders"] == 0:
        raise ValueError(
            "No Silver orders have quality_check_result PASSED/PASS. "
            "Inspect poc_catalog.default.silver_orders before Gold."
        )



@dataclass(frozen=True)
class GoldTableResult:
    """Outcome of one Gold SQL persistence."""

    sql_file: str
    target_table: str
    row_count: int
    duration_seconds: float


def get_or_create_spark(app_name: str) -> Tuple[SparkSession, bool]:
    """Reuse Databricks' session or create one under spark-submit."""
    active_session = SparkSession.getActiveSession()
    if active_session is not None:
        return active_session, False
    return SparkSession.builder.appName(app_name).getOrCreate(), True


def _sql_directory(sql_dir: str | None = None) -> Path:
    """Resolve the Gold SQL folder in scripts and Databricks notebooks."""
    marker = GOLD_JOBS[0][0]
    if sql_dir:
        candidate = Path(sql_dir)
        if (candidate / marker).is_file():
            return candidate
        raise FileNotFoundError(
            f"Gold SQL file {marker} was not found under {candidate}"
        )

    try:
        script_dir = Path(__file__).resolve().parent
        if (script_dir / marker).is_file():
            return script_dir
    except NameError:
        pass

    search_roots = [Path.cwd(), *Path.cwd().parents]
    for root in search_roots:
        for candidate in (root / "src" / "gold", root):
            if (candidate / marker).is_file():
                return candidate

    raise FileNotFoundError(
        "Gold SQL files were not found. In a Databricks notebook pass "
        "sql_dir='/Workspace/Repos/<user>/<repo>/src/gold' to create_gold_tables()."
    )


def load_sql(sql_file: str, sql_dir: str | None = None) -> str:
    """Read one Gold SQL script from the Gold source folder."""
    sql_path = _sql_directory(sql_dir) / sql_file
    if not sql_path.is_file():
        raise FileNotFoundError(f"Gold SQL file does not exist: {sql_path}")
    statement = sql_path.read_text(encoding="utf-8").strip()
    if not statement:
        raise ValueError(f"Gold SQL file is empty: {sql_path.name}")
    return statement


def execute_gold_sql(
    spark: SparkSession,
    sql_file: str,
    target_table: str,
    sql_dir: str | None = None,
) -> GoldTableResult:
    """Run one CREATE OR REPLACE TABLE script and log the resulting count."""
    started_at = datetime.now(timezone.utc)
    timer_started = perf_counter()
    print(
        f"[GOLD START] {started_at.isoformat()} | sql={sql_file} | "
        f"target={target_table}"
    )
    try:
        spark.sql(load_sql(sql_file, sql_dir))
        row_count = spark.table(target_table).count()
    except Exception as error:
        duration = perf_counter() - timer_started
        print(
            f"[GOLD FAILED] sql={sql_file} | target={target_table} | "
            f"duration_seconds={duration:.2f} | "
            f"error={type(error).__name__}: {error}"
        )
        raise

    duration = perf_counter() - timer_started
    completed_at = datetime.now(timezone.utc)
    print(
        f"[GOLD COMPLETE] {completed_at.isoformat()} | target={target_table} | "
        f"rows={row_count:,} | duration_seconds={duration:.2f}"
    )
    return GoldTableResult(sql_file, target_table, row_count, duration)


def create_gold_tables(
    spark: SparkSession,
    sql_dir: str | None = None,
) -> list[GoldTableResult]:
    """Persist all four Gold aggregation tables from Silver PASSED rows."""
    sql_folder = _sql_directory(sql_dir)
    print(f"[GOLD SQL DIR] {sql_folder}")
    assert_silver_ready(spark)
    results = [
        execute_gold_sql(spark, sql_file, target_table, str(sql_folder))
        for sql_file, target_table in GOLD_JOBS
    ]
    summary = " | ".join(
        f"{result.target_table.split('.')[-1]}={result.row_count:,}"
        for result in results
    )
    print(f"[GOLD PIPELINE COMPLETE] {summary}")
    return results


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build Gold Delta tables from Silver PASSED aggregations."
    )
    parser.add_argument(
        "--sql-dir",
        default=None,
        help="Folder containing the Gold .sql files (required in some notebooks).",
    )
    args, _ = parser.parse_known_args()

    spark, owns_session = get_or_create_spark("gold-create-tables")
    try:
        create_gold_tables(spark, args.sql_dir)
    finally:
        if owns_session:
            spark.stop()


if __name__ == "__main__":
    main()
