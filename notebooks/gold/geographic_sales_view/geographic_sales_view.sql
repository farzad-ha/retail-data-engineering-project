-- Databricks notebook source
-- DBTITLE 1,Create view
-- Creates vw_geographic_sales — sales volume, revenue, delivery KPIs, and review scores by city and state with lat/lng coordinates.

CREATE OR REPLACE VIEW second_data_engineering_project.gold.geographic_sales_view AS
WITH unique_geolocation AS (
    SELECT 
        geolocation_zip_code_prefix,
        AVG(geolocation_lat) AS avg_lat,
        AVG(geolocation_lng) AS avg_lng
    FROM second_data_engineering_project.silver.geolocation
    WHERE geolocation_lat IS NOT NULL 
      AND geolocation_lng IS NOT NULL
    GROUP BY geolocation_zip_code_prefix
)

SELECT
  m.customer_state,
  m.customer_city,
  AVG(g.avg_lat) AS average_sales_location_lat,
  AVG(g.avg_lng) AS average_sales_location_lng,
  COUNT(DISTINCT m.order_id) AS total_orders,
  COUNT(DISTINCT m.customer_unique_id) AS unique_customers,
  SUM(m.order_total_value) AS total_revenue,
  AVG(m.order_total_value) AS avg_order_value,
  AVG(delivery_days) AS avg_delivery_days,
  SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END) AS late_deliveries,
  ROUND(SUM(CASE WHEN is_late_delivery THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS late_delivery_rate_pct,
  AVG(review_score) AS avg_review_score,
  COUNT(DISTINCT product_category_name_english) AS unique_categories
FROM second_data_engineering_project.gold.master_table AS m
JOIN unique_geolocation AS g ON m.customer_zip_code_prefix = g.geolocation_zip_code_prefix
GROUP BY m.customer_state, m.customer_city