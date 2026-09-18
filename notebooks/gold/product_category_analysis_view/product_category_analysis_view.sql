-- Databricks notebook source
-- DBTITLE 1,Create view
-- Create product_category_analysis_review — revenue, review scores, product dimensions, and delivery stats per product category

CREATE OR REPLACE VIEW second_data_engineering_project.gold.product_category_analysis_review AS
SELECT
  product_category_name_english AS product_category,
  COUNT(*) AS total_items_sold,
  COUNT(DISTINCT product_id) AS unique_products,
  SUM(price) AS total_revenue,
  AVG(price) AS avg_item_price,
  AVG(freight_value) AS avg_freight,
  AVG(product_weight_g) AS avg_product_weight_g,
  AVG(product_volume_cm3) AS avg_product_volume_cm3,
  AVG(review_score) AS avg_review_score,
  SUM(CASE WHEN review_score = 5 THEN 1 ELSE 0 END) AS five_star_reviews,
  SUM(CASE WHEN review_score <= 2 THEN 1 ELSE 0 END) AS low_reviews,
  AVG(delivery_days) AS avg_delivery_days,
  SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END) AS late_deliveries,
  ROUND(SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS late_delivery_rate_pct
FROM second_data_engineering_project.gold.orders_master_table
WHERE product_category_name_english IS NOT NULL
GROUP BY product_category_name_english