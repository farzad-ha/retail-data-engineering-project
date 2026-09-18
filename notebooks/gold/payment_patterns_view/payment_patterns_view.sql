-- Databricks notebook source
-- DBTITLE 1,Create view
-- Create payment_patterns view — payment method distribution, installment usage, and payment volume by product category and customer state

CREATE OR REPLACE VIEW second_data_engineering_project.gold.payment_patterns_view AS
SELECT
  primary_payment_type AS payment_type,
  product_category_name_english AS product_category,
  customer_state,
  COUNT(*) AS total_orders,
  AVG(total_payment_value) AS avg_payment_value,
  AVG(price) AS avg_order_price,
  AVG(max_installments) AS avg_installments,
  SUM(CASE WHEN max_installments > 1 THEN 1 ELSE 0 END) AS installment_orders,
  ROUND(SUM(CASE WHEN max_installments > 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS installment_rate_pct,
  AVG(payment_count) AS avg_payment_sequential_count,
  SUM(total_payment_value) AS total_payment_volume
FROM second_data_engineering_project.gold.orders_master_table
WHERE primary_payment_type IS NOT NULL
GROUP BY primary_payment_type, product_category_name_english, customer_state