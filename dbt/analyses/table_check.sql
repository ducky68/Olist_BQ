-- Simple test query to check if all staging tables exist and have data
-- Run this first to debug why only stg_products is showing up

SELECT 
  'stg_customers' as table_name,
  COUNT(*) as record_count
FROM `project-olist-470307.dbt_olist_stg.stg_customers`

UNION ALL

SELECT 
  'stg_geolocation' as table_name,
  COUNT(*) as record_count
FROM `project-olist-470307.dbt_olist_stg.stg_geolocation`

UNION ALL

SELECT 
  'stg_order_items' as table_name,
  COUNT(*) as record_count
FROM `project-olist-470307.dbt_olist_stg.stg_order_items`

UNION ALL

SELECT 
  'stg_order_payments' as table_name,
  COUNT(*) as record_count
FROM `project-olist-470307.dbt_olist_stg.stg_order_payments`

UNION ALL

SELECT 
  'stg_order_reviews' as table_name,
  COUNT(*) as record_count
FROM `project-olist-470307.dbt_olist_stg.stg_order_reviews`

UNION ALL

SELECT 
  'stg_orders' as table_name,
  COUNT(*) as record_count
FROM `project-olist-470307.dbt_olist_stg.stg_orders`

UNION ALL

SELECT 
  'stg_product_category_name_translation' as table_name,
  COUNT(*) as record_count
FROM `project-olist-470307.dbt_olist_stg.stg_product_category_name_translation`

UNION ALL

SELECT 
  'stg_products' as table_name,
  COUNT(*) as record_count
FROM `project-olist-470307.dbt_olist_stg.stg_products`

UNION ALL

SELECT 
  'stg_sellers' as table_name,
  COUNT(*) as record_count
FROM `project-olist-470307.dbt_olist_stg.stg_sellers`

ORDER BY table_name;
