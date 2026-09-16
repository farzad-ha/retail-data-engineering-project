# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Materialized View Test
# MAGIC
# MAGIC Cloned from Master_table_sql to test whether CREATE MATERIALIZED VIEW works in Databricks Free Edition.

# COMMAND ----------

# DBTITLE 1,Start timer
import time
start_time = time.time()
print(f"⏱️  SQL materialized view execution started at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

# COMMAND ----------

# DBTITLE 1,Create materialized view (test)
# MAGIC %sql
# MAGIC CREATE OR REPLACE MATERIALIZED VIEW second_data_engineering_project.gold.orders_master_mv_test
# MAGIC SCHEDULE EVERY 1 HOUR
# MAGIC AS
# MAGIC WITH payments_agg AS (
# MAGIC   SELECT 
# MAGIC     order_id,
# MAGIC     SUM(payment_value) as total_payment_value,
# MAGIC     COUNT(payment_sequential) as payment_count,
# MAGIC     MAX(payment_installments) as max_installments,
# MAGIC     FIRST(payment_type) as primary_payment_type
# MAGIC   FROM second_data_engineering_project.silver.payments
# MAGIC   GROUP BY order_id
# MAGIC ),
# MAGIC geo_lookup AS (
# MAGIC   -- Single aggregation for both customer and seller geolocation lookups
# MAGIC   SELECT 
# MAGIC     geolocation_zip_code_prefix,
# MAGIC     FIRST(geolocation_lat) as geo_lat,
# MAGIC     FIRST(geolocation_lng) as geo_lng
# MAGIC   FROM second_data_engineering_project.silver.geolocation
# MAGIC   GROUP BY geolocation_zip_code_prefix
# MAGIC )
# MAGIC SELECT
# MAGIC   -- Primary Keys
# MAGIC   oi.order_id,
# MAGIC   oi.order_item_id,
# MAGIC   
# MAGIC   -- Order Information
# MAGIC   o.order_status,
# MAGIC   o.order_purchase_timestamp,
# MAGIC   o.order_approved_at,
# MAGIC   o.order_delivered_carrier_date,
# MAGIC   o.order_delivered_customer_date,
# MAGIC   o.order_estimated_delivery_date,
# MAGIC   DATE(o.order_purchase_timestamp) as order_date,
# MAGIC   YEAR(o.order_purchase_timestamp) as order_year,
# MAGIC   MONTH(o.order_purchase_timestamp) as order_month,
# MAGIC   QUARTER(o.order_purchase_timestamp) as order_quarter,
# MAGIC   DAYOFWEEK(o.order_purchase_timestamp) as order_day_of_week,
# MAGIC   
# MAGIC   -- Customer Information
# MAGIC   c.customer_id,
# MAGIC   c.customer_unique_id,
# MAGIC   c.customer_city,
# MAGIC   c.customer_state,
# MAGIC   c.customer_zip_code_prefix,
# MAGIC   cg.geo_lat as customer_lat,
# MAGIC   cg.geo_lng as customer_lng,
# MAGIC   
# MAGIC   -- Product Information
# MAGIC   p.product_id,
# MAGIC   p.product_category_name,
# MAGIC   ct.product_category_name_english,
# MAGIC   p.product_name_lenght,
# MAGIC   p.product_description_lenght,
# MAGIC   p.product_photos_qty,
# MAGIC   p.product_weight_g,
# MAGIC   p.product_length_cm,
# MAGIC   p.product_height_cm,
# MAGIC   p.product_width_cm,
# MAGIC   (p.product_length_cm * p.product_width_cm * p.product_height_cm) as product_volume_cm3,
# MAGIC   
# MAGIC   -- Seller Information
# MAGIC   s.seller_id,
# MAGIC   s.seller_city,
# MAGIC   s.seller_state,
# MAGIC   s.seller_zip_code_prefix,
# MAGIC   sg.geo_lat as seller_lat,
# MAGIC   sg.geo_lng as seller_lng,
# MAGIC   
# MAGIC   -- Order Item Details
# MAGIC   oi.price,
# MAGIC   oi.freight_value,
# MAGIC   (oi.price + oi.freight_value) as order_total_value,
# MAGIC   oi.shipping_limit_date,
# MAGIC   
# MAGIC   -- Payment Information
# MAGIC   pa.total_payment_value,
# MAGIC   pa.payment_count,
# MAGIC   pa.primary_payment_type,
# MAGIC   pa.max_installments,
# MAGIC   
# MAGIC   -- Review Information
# MAGIC   r.review_id,
# MAGIC   r.review_score,
# MAGIC   r.review_comment_title,
# MAGIC   r.review_comment_message,
# MAGIC   r.review_creation_date,
# MAGIC   r.review_answer_timestamp,
# MAGIC   CASE WHEN r.review_id IS NOT NULL THEN TRUE ELSE FALSE END as has_review,
# MAGIC   
# MAGIC   -- Calculated Metrics
# MAGIC   DATEDIFF(DAY, o.order_purchase_timestamp, o.order_delivered_customer_date) as delivery_days,
# MAGIC   DATEDIFF(DAY, o.order_purchase_timestamp, o.order_estimated_delivery_date) as estimated_delivery_days,
# MAGIC   CASE 
# MAGIC     WHEN o.order_delivered_customer_date IS NOT NULL 
# MAGIC     THEN DATEDIFF(DAY, o.order_delivered_customer_date, o.order_estimated_delivery_date)
# MAGIC     ELSE NULL 
# MAGIC   END as delivery_performance,
# MAGIC   CASE 
# MAGIC     WHEN DATEDIFF(DAY, o.order_delivered_customer_date, o.order_estimated_delivery_date) < 0 
# MAGIC     THEN TRUE 
# MAGIC     ELSE FALSE 
# MAGIC   END as is_late_delivery
# MAGIC   
# MAGIC FROM second_data_engineering_project.silver.order_items oi
# MAGIC INNER JOIN second_data_engineering_project.silver.orders o ON oi.order_id = o.order_id
# MAGIC LEFT JOIN second_data_engineering_project.silver.customers c ON o.customer_id = c.customer_id
# MAGIC LEFT JOIN geo_lookup cg ON c.customer_zip_code_prefix = cg.geolocation_zip_code_prefix
# MAGIC LEFT JOIN second_data_engineering_project.silver.products p ON oi.product_id = p.product_id
# MAGIC LEFT JOIN second_data_engineering_project.silver.product_category_name_translation ct ON p.product_category_name = ct.product_category_name
# MAGIC LEFT JOIN second_data_engineering_project.silver.sellers s ON oi.seller_id = s.seller_id
# MAGIC LEFT JOIN geo_lookup sg ON s.seller_zip_code_prefix = sg.geolocation_zip_code_prefix
# MAGIC LEFT JOIN payments_agg pa ON oi.order_id = pa.order_id
# MAGIC LEFT JOIN second_data_engineering_project.silver.reviews r ON oi.order_id = r.order_id

# COMMAND ----------

# DBTITLE 1,Count records
# MAGIC %sql
# MAGIC SELECT COUNT(*) as total_records FROM second_data_engineering_project.gold.orders_master_mv_test

# COMMAND ----------

# DBTITLE 1,End timer
end_time = time.time()
execution_time = end_time - start_time
print(f"✅ Materialized view created: second_data_engineering_project.gold.orders_master_mv_test")
print(f"⏱️  Total execution time: {execution_time:.2f} seconds ({execution_time/60:.2f} minutes)")