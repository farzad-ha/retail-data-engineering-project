-- Databricks notebook source
-- DBTITLE 1,Create view
-- Create delivery_performance_view — average delivery days, late delivery rate, and review scores by seller, category, and destination state

CREATE OR REPLACE VIEW second_data_engineering_project.gold.delivery_performance_view AS
SELECT
  seller_id,
  seller_city,
  seller_state,
  product_category_name_english AS product_category,
  customer_state AS destination_state,
  COUNT(*) AS total_orders,
  AVG(delivery_days) AS avg_delivery_days,
  MIN(delivery_days) AS min_delivery_days,
  MAX(delivery_days) AS max_delivery_days,
  AVG(estimated_delivery_days) AS avg_estimated_delivery_days,
  AVG(delivery_performance) AS avg_delivery_performance,
  SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END) AS late_deliveries,
  ROUND(SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS late_delivery_rate_pct,
  AVG(review_score) AS avg_review_score
FROM second_data_engineering_project.gold.orders_master_table
WHERE order_status = 'delivered'
GROUP BY seller_id, seller_city, seller_state, product_category_name_english, customer_state