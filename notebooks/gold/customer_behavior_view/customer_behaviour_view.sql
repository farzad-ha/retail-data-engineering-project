-- Databricks notebook source
-- DBTITLE 1,Create view

-- Create customer_behaviour view table — repeat customer flags, total spent, lifespan, category diversity, and preferred payment type per customer

CREATE OR REPLACE VIEW second_data_engineering_project.gold.vw_customer_behaviour AS
SELECT
  customer_unique_id,
  customer_city,
  customer_state,
  COUNT(DISTINCT order_id) AS total_orders,
  MIN(order_date) AS first_purchase_date,
  MAX(order_date) AS last_purchase_date,
  DATEDIFF(DAY, MIN(order_date), MAX(order_date)) AS customer_lifespan_days,
  SUM(order_total_value) AS total_spent,
  AVG(order_total_value) AS avg_order_value,
  MAX(order_total_value) AS max_order_value,
  COUNT(DISTINCT product_category_name_english) AS unique_categories_bought,
  AVG(review_score) AS avg_review_score,
  SUM(CASE WHEN review_score <= 2 THEN 1 ELSE 0 END) AS negative_reviews,
  primary_payment_type AS preferred_payment_type,
  CASE WHEN COUNT(DISTINCT order_id) > 1 THEN TRUE ELSE FALSE END AS is_repeat_customer
FROM second_data_engineering_project.gold.orders_master_table
GROUP BY customer_unique_id, customer_city, customer_state, primary_payment_type