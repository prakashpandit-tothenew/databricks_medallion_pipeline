-- Gold customer revenue: one row per PASSED customer, including zero-order customers.
-- AI rationale: left-join PASSED orders so Inactive customers still appear;
-- lifetime_value_actual is computed spend, not the source lifetime_value field.
-- Validation: grain is customer_id; total_orders is COUNT(DISTINCT order_id).

CREATE OR REPLACE TABLE poc_catalog.default.gold_revenue_by_customer
USING DELTA AS
SELECT
  c.customer_id,
  c.customer_name,
  c.customer_segment,
  COUNT(DISTINCT o.order_id) AS total_orders,
  CAST(COALESCE(SUM(o.total_amount), 0) AS DECIMAL(18, 2)) AS total_revenue,
  CAST(
    CASE
      WHEN COUNT(DISTINCT o.order_id) = 0 THEN 0
      ELSE SUM(o.total_amount) / COUNT(DISTINCT o.order_id)
    END AS DECIMAL(18, 2)
  ) AS avg_order_value,
  CAST(COALESCE(SUM(o.total_amount), 0) AS DECIMAL(18, 2)) AS lifetime_value_actual
FROM poc_catalog.default.silver_customers AS c
LEFT JOIN poc_catalog.default.silver_orders AS o
  ON c.customer_id = o.customer_id
 AND o.quality_check_result = 'PASSED'
WHERE c.quality_check_result = 'PASSED'
GROUP BY
  c.customer_id,
  c.customer_name,
  c.customer_segment
;
