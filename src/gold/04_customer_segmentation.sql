-- Gold segmentation: recategorize PASSED customers from actual order behavior.
-- AI rationale: source customer_segment is descriptive; Gold uses spend/order
-- rules so Inactive vs One-Time vs Repeat vs High-Value reflects Silver facts.
-- Rules: 0 orders = Inactive; 1 order = One-Time; 2+ orders and spend >= 5000
-- = High-Value; otherwise Repeat. 5000 matches the High-Value LTV floor used
-- in sample generation.
-- Validation: grain is derived_segment; customer_count is COUNT(DISTINCT customer_id).

CREATE OR REPLACE TABLE poc_catalog.default.gold_customer_segmentation
USING DELTA AS
WITH customer_activity AS (
  SELECT
    c.customer_id,
    COUNT(DISTINCT o.order_id) AS total_orders,
    CAST(COALESCE(SUM(o.total_amount), 0) AS DECIMAL(18, 2)) AS total_revenue
  FROM poc_catalog.default.silver_customers AS c
  LEFT JOIN poc_catalog.default.silver_orders AS o
    ON c.customer_id = o.customer_id
   AND o.quality_check_result = 'PASSED'
  WHERE c.quality_check_result = 'PASSED'
  GROUP BY c.customer_id
),
segmented AS (
  SELECT
    customer_id,
    total_orders,
    total_revenue,
    CASE
      WHEN total_orders = 0 THEN 'Inactive'
      WHEN total_orders = 1 THEN 'One-Time'
      WHEN total_revenue >= 5000 THEN 'High-Value'
      ELSE 'Repeat'
    END AS derived_segment
  FROM customer_activity
)
SELECT
  derived_segment AS customer_segment,
  COUNT(DISTINCT customer_id) AS customer_count,
  CAST(SUM(total_orders) AS BIGINT) AS total_orders,
  CAST(SUM(total_revenue) AS DECIMAL(18, 2)) AS total_revenue,
  CAST(
    SUM(total_revenue) / COUNT(DISTINCT customer_id) AS DECIMAL(18, 2)
  ) AS avg_revenue_per_customer,
  CAST(
    CASE
      WHEN SUM(total_orders) = 0 THEN 0
      ELSE SUM(total_revenue) / SUM(total_orders)
    END AS DECIMAL(18, 2)
  ) AS avg_order_value
FROM segmented
GROUP BY derived_segment
;
