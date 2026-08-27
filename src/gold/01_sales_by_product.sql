-- Gold product sales: one row per PASSED product with qualifying order activity.
-- AI rationale: Silver flags stay in Silver; Gold measures use only PASSED orders
-- and PASSED products so defective keys never enter BI totals.
-- Validation: grain is product_id; total_revenue is SUM(total_amount);
-- avg_order_value is total_revenue / COUNT(DISTINCT order_id).

CREATE OR REPLACE TABLE poc_catalog.default.gold_sales_by_product
USING DELTA AS
SELECT
  p.product_id,
  p.product_name,
  p.category,
  COUNT(DISTINCT o.order_id) AS total_orders,
  CAST(SUM(o.total_amount) AS DECIMAL(18, 2)) AS total_revenue,
  CAST(
    SUM(o.total_amount) / COUNT(DISTINCT o.order_id) AS DECIMAL(18, 2)
  ) AS avg_order_value
FROM poc_catalog.default.silver_orders AS o
INNER JOIN poc_catalog.default.silver_products AS p
  ON o.product_id = p.product_id
WHERE upper(trim(o.quality_check_result)) IN ('PASSED', 'PASS')
  AND upper(trim(p.quality_check_result)) IN ('PASSED', 'PASS')
GROUP BY
  p.product_id,
  p.product_name,
  p.category
;
