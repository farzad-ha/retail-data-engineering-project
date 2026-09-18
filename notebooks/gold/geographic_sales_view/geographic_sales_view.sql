-- Databricks notebook source
-- DBTITLE 1,Create view
-- Creates vw_geographic_sales — sales volume, revenue, delivery KPIs, and review scores by city and state with lat/lng coordinates.

CREATE OR REPLACE VIEW second_data_engineering_project.gold.vw_geographic_sales AS
SELECT
  customer_state,
  customer_city,
  customer_lat,
  customer_lng,
  COUNT(DISTINCT order_id) AS total_orders,
  COUNT(DISTINCT customer_unique_id) AS unique_customers,
  SUM(order_total_value) AS total_revenue,
  AVG(order_total_value) AS avg_order_value,
  AVG(delivery_days) AS avg_delivery_days,
  SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END) AS late_deliveries,
  ROUND(SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS late_delivery_rate_pct,
  AVG(review_score) AS avg_review_score,
  COUNT(DISTINCT product_category_name_english) AS unique_categories
FROM second_data_engineering_project.gold.orders_master_table
WHERE customer_lat IS NOT NULL AND customer_lng IS NOT NULL
GROUP BY customer_state, customer_city, customer_lat, customer_lng