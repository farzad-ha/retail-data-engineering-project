-- Databricks notebook source
-- DBTITLE 1,Create master table
CREATE OR REFRESH STREAMING TABLE second_data_engineering_project.gold.master_table
PARTITIONED BY (order_year, order_month)
AS
WITH payments_agg AS (
  SELECT 
    order_id,
    SUM(payment_value) as total_payment_value,
    COUNT(payment_sequential) as payment_count,
    MAX(payment_installments) as max_installments,
    COLLECT_SET(payment_type) as payment_types_list
  FROM second_data_engineering_project.silver.payments
  GROUP BY order_id
)
SELECT
  oi.*,
  o.* EXCEPT(order_id), 
  c.* EXCEPT(customer_id),
  p.* EXCEPT(product_id),
  s.* EXCEPT(seller_id),
  r.* EXCEPT(order_id),
  ct.product_category_name_english,
  pa.* EXCEPT(order_id),

  DATE(o.order_purchase_timestamp) as order_date,
  YEAR(o.order_purchase_timestamp) as order_year,
  MONTH(o.order_purchase_timestamp) as order_month,
  QUARTER(o.order_purchase_timestamp) as order_quarter,
  DAYOFWEEK(o.order_purchase_timestamp) as order_day_of_week,
  (p.product_length_cm * p.product_width_cm * p.product_height_cm) as product_volume_cm3,
  (oi.price + oi.freight_value) as order_total_value,
  CASE WHEN r.review_id IS NOT NULL THEN TRUE ELSE FALSE END as has_review,
  DATEDIFF(o.order_delivered_customer_date, o.order_purchase_timestamp) as delivery_days,
  DATEDIFF(o.order_estimated_delivery_date, o.order_purchase_timestamp) as estimated_delivery_days,
  CASE 
    WHEN o.order_delivered_customer_date IS NOT NULL 
    THEN DATEDIFF(o.order_estimated_delivery_date, o.order_delivered_customer_date)
    ELSE NULL 
  END as delivery_performance,
  CASE 
    WHEN DATEDIFF(o.order_estimated_delivery_date, o.order_delivered_customer_date) < 0 
    THEN TRUE 
    ELSE FALSE 
  END as is_late_delivery
  
FROM STREAM second_data_engineering_project.silver.order_items AS oi
INNER JOIN second_data_engineering_project.silver.orders AS o ON oi.order_id = o.order_id
LEFT JOIN second_data_engineering_project.silver.customers AS c ON o.customer_id = c.customer_id
LEFT JOIN second_data_engineering_project.silver.products AS p ON oi.product_id = p.product_id
LEFT JOIN second_data_engineering_project.silver.product_category_name_translation AS ct ON p.product_category_name = ct.product_category_name
LEFT JOIN second_data_engineering_project.silver.sellers AS s ON oi.seller_id = s.seller_id
LEFT JOIN payments_agg AS pa ON oi.order_id = pa.order_id
LEFT JOIN second_data_engineering_project.silver.reviews AS r ON oi.order_id = r.order_id;