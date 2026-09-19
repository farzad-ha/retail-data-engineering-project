-- Databricks notebook source
-- DBTITLE 1,Create view
-- Create vw_sales_revenue_summary — monthly revenue, order count, AOV, and unique customers by product category and customer state 
CREATE OR REPLACE VIEW second_data_engineering_project.gold.sales_revenue_summary_view AS
SELECT
  order_year,
  order_month,
  order_quarter,
  product_category_name_english AS product_category,
  customer_state,
  COUNT(DISTINCT order_id) AS order_count,
  COUNT(DISTINCT customer_unique_id) AS unique_customers,
  SUM(price) AS total_item_revenue,
  SUM(freight_value) AS total_freight,
  SUM(order_total_value) AS total_order_value,
  AVG(order_total_value) AS avg_order_value,
  SUM(total_payment_value) AS total_payment_received,
  COUNT(DISTINCT product_id) AS unique_products_sold
FROM second_data_engineering_project.gold.orders_master_table
WHERE order_status IN ('delivered', 'shipped', 'approved')
GROUP BY order_year, order_month, order_quarter, product_category_name_english, customer_state