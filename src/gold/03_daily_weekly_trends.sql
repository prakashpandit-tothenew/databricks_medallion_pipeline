-- Gold time trends: daily and weekly revenue from PASSED orders.
-- AI rationale: one table with period_grain keeps dashboard queries simple
-- without a second Gold object. Week start uses Spark date_trunc('WEEK').
-- Validation: grain is (period_grain, period_start); revenue is SUM(total_amount).

CREATE OR REPLACE TABLE poc_catalog.default.gold_daily_weekly_trends
USING DELTA AS
SELECT
  'daily' AS period_grain,
  o.order_date AS period_start,
  COUNT(DISTINCT o.order_id) AS total_orders,
  CAST(SUM(o.total_amount) AS DECIMAL(18, 2)) AS total_revenue,
  CAST(
    SUM(o.total_amount) / COUNT(DISTINCT o.order_id) AS DECIMAL(18, 2)
  ) AS avg_order_value
FROM poc_catalog.default.silver_orders AS o
WHERE o.quality_check_result = 'PASSED'
GROUP BY o.order_date

UNION ALL

SELECT
  'weekly' AS period_grain,
  CAST(DATE_TRUNC('WEEK', o.order_date) AS DATE) AS period_start,
  COUNT(DISTINCT o.order_id) AS total_orders,
  CAST(SUM(o.total_amount) AS DECIMAL(18, 2)) AS total_revenue,
  CAST(
    SUM(o.total_amount) / COUNT(DISTINCT o.order_id) AS DECIMAL(18, 2)
  ) AS avg_order_value
FROM poc_catalog.default.silver_orders AS o
WHERE o.quality_check_result = 'PASSED'
GROUP BY CAST(DATE_TRUNC('WEEK', o.order_date) AS DATE)
;
