-- Databricks notebook source
-- DBTITLE 1,Create view
-- Creates review_satisfaction_view — review score distribution, comment rates, and delivery correlation by month and product category

CREATE OR REPLACE VIEW second_data_engineering_project.gold.review_satisfaction_view AS
SELECT
  order_year,
  order_month,
  product_category_name_english AS product_category,
  review_score,
  COUNT(*) AS review_count,
  COUNT(DISTINCT order_id) AS orders_with_reviews,
  SUM(CASE WHEN review_comment_title IS NOT NULL THEN 1 ELSE 0 END) AS reviews_with_title,
  SUM(CASE WHEN review_comment_message IS NOT NULL THEN 1 ELSE 0 END) AS reviews_with_comment,
  AVG(delivery_days) AS avg_delivery_days_for_score,
  AVG(delivery_performance) AS avg_delivery_performance_for_score,
  SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END) AS late_deliveries_for_score,
  AVG(price) AS avg_price_for_score
FROM second_data_engineering_project.gold.orders_master_table
WHERE review_score IS NOT NULL
GROUP BY order_year, order_month, product_category_name_english, review_score