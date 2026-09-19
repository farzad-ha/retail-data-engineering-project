-- Databricks notebook source
-- DBTITLE 1,Create view
-- Create seller_performance_view — revenue, order count, review scores, and late delivery rate per seller

CREATE OR REPLACE VIEW second_data_engineering_project.gold.seller_performance_view AS
SELECT
  seller_id,
  seller_city,
  seller_state,
  COUNT(DISTINCT order_id) AS total_orders,
  COUNT(DISTINCT product_id) AS unique_products_sold,
  SUM(price) AS total_revenue,
  AVG(price) AS avg_item_price,
  SUM(freight_value) AS total_freight_revenue,
  AVG(review_score) AS avg_review_score,
  SUM(CASE WHEN review_score = 5 THEN 1 ELSE 0 END) AS five_star_count,
  SUM(CASE WHEN review_score <= 2 THEN 1 ELSE 0 END) AS low_rating_count,
  AVG(delivery_days) AS avg_delivery_days,
  SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END) AS late_deliveries,
  ROUND(SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS late_delivery_rate_pct,
  MIN(order_date) AS first_sale_date,
  MAX(order_date) AS last_sale_date
FROM second_data_engineering_project.gold.orders_master_table
WHERE seller_id IS NOT NULL
GROUP BY seller_id, seller_city, seller_state