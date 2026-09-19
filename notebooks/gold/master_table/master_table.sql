-- Databricks notebook source
-- DBTITLE 1,Create master table
CREATE OR REPLACE TABLE second_data_engineering_project.gold.orders_master_table
PARTITIONED BY (order_year, order_month)
AS
WITH payments_agg AS (
  SELECT 
    order_id,
    SUM(payment_value) as total_payment_value,
    COUNT(payment_sequential) as payment_count,
    MAX(payment_installments) as max_installments,
    FIRST(payment_type) as primary_payment_type
  FROM second_data_engineering_project.silver.payments
  GROUP BY order_id
),
geo_lookup AS (
  -- Single aggregation for both customer and seller geolocation lookups
  SELECT 
    geolocation_zip_code_prefix,
    FIRST(geolocation_lat) as geo_lat,
    FIRST(geolocation_lng) as geo_lng
  FROM second_data_engineering_project.silver.geolocation
  GROUP BY geolocation_zip_code_prefix
)
SELECT
  -- Primary Keys
  oi.order_id,
  oi.order_item_id,
  
  -- Order Information
  o.order_status,
  o.order_purchase_timestamp,
  o.order_approved_at,
  o.order_delivered_carrier_date,
  o.order_delivered_customer_date,
  o.order_estimated_delivery_date,
  DATE(o.order_purchase_timestamp) as order_date,
  YEAR(o.order_purchase_timestamp) as order_year,
  MONTH(o.order_purchase_timestamp) as order_month,
  QUARTER(o.order_purchase_timestamp) as order_quarter,
  DAYOFWEEK(o.order_purchase_timestamp) as order_day_of_week,
  
  -- Customer Information
  c.customer_id,
  c.customer_unique_id,
  c.customer_city,
  c.customer_state,
  c.customer_zip_code_prefix,
  cg.geo_lat as customer_lat,
  cg.geo_lng as customer_lng,
  
  -- Product Information
  p.product_id,
  p.product_category_name,
  ct.product_category_name_english,
  p.product_name_lenght,
  p.product_description_lenght,
  p.product_photos_qty,
  p.product_weight_g,
  p.product_length_cm,
  p.product_height_cm,
  p.product_width_cm,
  (p.product_length_cm * p.product_width_cm * p.product_height_cm) as product_volume_cm3,
  
  -- Seller Information
  s.seller_id,
  s.seller_city,
  s.seller_state,
  s.seller_zip_code_prefix,
  sg.geo_lat as seller_lat,
  sg.geo_lng as seller_lng,
  
  -- Order Item Details
  oi.price,
  oi.freight_value,
  (oi.price + oi.freight_value) as order_total_value,
  oi.shipping_limit_date,
  
  -- Payment Information
  pa.total_payment_value,
  pa.payment_count,
  pa.primary_payment_type,
  pa.max_installments,
  
  -- Review Information
  r.review_id,
  r.review_score,
  r.review_comment_title,
  r.review_comment_message,
  r.review_creation_date,
  r.review_answer_timestamp,
  CASE WHEN r.review_id IS NOT NULL THEN TRUE ELSE FALSE END as has_review,
  
  -- Calculated Metrics
  DATEDIFF(DAY, o.order_purchase_timestamp, o.order_delivered_customer_date) as delivery_days,
  DATEDIFF(DAY, o.order_purchase_timestamp, o.order_estimated_delivery_date) as estimated_delivery_days,
  CASE 
    WHEN o.order_delivered_customer_date IS NOT NULL 
    THEN DATEDIFF(DAY, o.order_delivered_customer_date, o.order_estimated_delivery_date)
    ELSE NULL 
  END as delivery_performance,
  CASE 
    WHEN DATEDIFF(DAY, o.order_delivered_customer_date, o.order_estimated_delivery_date) < 0 
    THEN TRUE 
    ELSE FALSE 
  END as is_late_delivery
  
FROM second_data_engineering_project.silver.order_items oi
INNER JOIN second_data_engineering_project.silver.orders o ON oi.order_id = o.order_id
LEFT JOIN second_data_engineering_project.silver.customers c ON o.customer_id = c.customer_id
LEFT JOIN geo_lookup cg ON c.customer_zip_code_prefix = cg.geolocation_zip_code_prefix
LEFT JOIN second_data_engineering_project.silver.products p ON oi.product_id = p.product_id
LEFT JOIN second_data_engineering_project.silver.product_category_name_translation ct ON p.product_category_name = ct.product_category_name
LEFT JOIN second_data_engineering_project.silver.sellers s ON oi.seller_id = s.seller_id
LEFT JOIN geo_lookup sg ON s.seller_zip_code_prefix = sg.geolocation_zip_code_prefix
LEFT JOIN payments_agg pa ON oi.order_id = pa.order_id
LEFT JOIN second_data_engineering_project.silver.reviews r ON oi.order_id = r.order_id