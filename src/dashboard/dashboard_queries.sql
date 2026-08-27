-- Databricks SQL dashboard queries against Gold Delta tables.
-- Bind a SQL warehouse, then create visualizations:
--   1) Bar: Top 10 Products by Revenue
--   2) Bar/histogram: Customer Revenue Distribution
--   3) Pie: Segmentation
--   4) Line: Daily revenue (extra)
-- Use only PASSED-backed Gold measures. Do not query Bronze for BI.

-- ---------------------------------------------------------------------------
-- 1. Top 10 Products by Revenue
-- Visualization: horizontal or vertical bar
-- X: product_name  Y: total_revenue
-- ---------------------------------------------------------------------------
SELECT
  product_id,
  product_name,
  category,
  total_orders,
  total_revenue,
  avg_order_value
FROM poc_catalog.default.gold_sales_by_product
ORDER BY total_revenue DESC
LIMIT 10;

-- ---------------------------------------------------------------------------
-- 2. Customer Revenue Distribution Histogram
-- Visualization: bar chart treated as a histogram
-- X: revenue_bucket  Y: customer_count
-- Buckets avoid listing customer names (privacy-safe).
-- ---------------------------------------------------------------------------
SELECT
  CASE
    WHEN lifetime_value_actual = 0 THEN '00_zero'
    WHEN lifetime_value_actual < 100 THEN '01_0_to_99'
    WHEN lifetime_value_actual < 500 THEN '02_100_to_499'
    WHEN lifetime_value_actual < 1000 THEN '03_500_to_999'
    WHEN lifetime_value_actual < 5000 THEN '04_1000_to_4999'
    WHEN lifetime_value_actual < 10000 THEN '05_5000_to_9999'
    ELSE '06_10000_plus'
  END AS revenue_bucket,
  COUNT(*) AS customer_count,
  CAST(SUM(lifetime_value_actual) AS DECIMAL(18, 2)) AS bucket_revenue
FROM poc_catalog.default.gold_revenue_by_customer
GROUP BY 1
ORDER BY 1;

-- ---------------------------------------------------------------------------
-- 3. Segmentation Pie Chart
-- Visualization: pie
-- Slice label: customer_segment  Value: customer_count
-- ---------------------------------------------------------------------------
SELECT
  customer_segment,
  customer_count,
  total_orders,
  total_revenue,
  avg_revenue_per_customer,
  avg_order_value
FROM poc_catalog.default.gold_customer_segmentation
ORDER BY customer_count DESC;

-- ---------------------------------------------------------------------------
-- 4. Daily revenue trend (additional dashboard tile)
-- Visualization: line
-- X: period_start  Y: total_revenue
-- ---------------------------------------------------------------------------
SELECT
  period_start,
  total_orders,
  total_revenue,
  avg_order_value
FROM poc_catalog.default.gold_daily_weekly_trends
WHERE period_grain = 'daily'
ORDER BY period_start;

-- ---------------------------------------------------------------------------
-- 5. Silver quality scorecard (pipeline health, not a sales KPI)
-- Visualization: table or stacked bar
-- ---------------------------------------------------------------------------
SELECT
  dataset,
  check_name,
  total_row_count,
  passed_count,
  failed_count,
  pass_percentage,
  measured_at
FROM poc_catalog.default.silver_quality_summary
ORDER BY dataset, check_name;
