-- Duplicate Summary Report for Olist Staging Tables
-- Run this query in BigQuery Console to get duplicate statistics

WITH duplicate_stats AS (
  -- Customers
  SELECT 
    'stg_customers' as table_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN had_duplicates THEN 1 ELSE 0 END) as records_with_duplicates,
    ROUND(SUM(CASE WHEN had_duplicates THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as duplicate_percentage
  FROM `project-olist-470307.dbt_olist_stg.stg_customers`
  
  UNION ALL
  
  -- Geolocation
  SELECT 
    'stg_geolocation' as table_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN had_duplicates THEN 1 ELSE 0 END) as records_with_duplicates,
    ROUND(SUM(CASE WHEN had_duplicates THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as duplicate_percentage
  FROM `project-olist-470307.dbt_olist_stg.stg_geolocation`
  
  UNION ALL
  
  -- Order Items
  SELECT 
    'stg_order_items' as table_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN had_duplicates THEN 1 ELSE 0 END) as records_with_duplicates,
    ROUND(SUM(CASE WHEN had_duplicates THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as duplicate_percentage
  FROM `project-olist-470307.dbt_olist_stg.stg_order_items`
  
  UNION ALL
  
  -- Order Payments
  SELECT 
    'stg_order_payments' as table_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN had_duplicates THEN 1 ELSE 0 END) as records_with_duplicates,
    ROUND(SUM(CASE WHEN had_duplicates THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as duplicate_percentage
  FROM `project-olist-470307.dbt_olist_stg.stg_order_payments`
  
  UNION ALL
  
  -- Order Reviews
  SELECT 
    'stg_order_reviews' as table_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN had_duplicates THEN 1 ELSE 0 END) as records_with_duplicates,
    ROUND(SUM(CASE WHEN had_duplicates THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as duplicate_percentage
  FROM `project-olist-470307.dbt_olist_stg.stg_order_reviews`
  
  UNION ALL
  
  -- Orders
  SELECT 
    'stg_orders' as table_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN had_duplicates THEN 1 ELSE 0 END) as records_with_duplicates,
    ROUND(SUM(CASE WHEN had_duplicates THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as duplicate_percentage
  FROM `project-olist-470307.dbt_olist_stg.stg_orders`
  
  UNION ALL
  
  -- Product Category Translation
  SELECT 
    'stg_product_category_name_translation' as table_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN had_duplicates THEN 1 ELSE 0 END) as records_with_duplicates,
    ROUND(SUM(CASE WHEN had_duplicates THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as duplicate_percentage
  FROM `project-olist-470307.dbt_olist_stg.stg_product_category_name_translation`
  
  UNION ALL
  
  -- Products
  SELECT 
    'stg_products' as table_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN had_duplicates THEN 1 ELSE 0 END) as records_with_duplicates,
    ROUND(SUM(CASE WHEN had_duplicates THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as duplicate_percentage
  FROM `project-olist-470307.dbt_olist_stg.stg_products`
  
  UNION ALL
  
  -- Sellers
  SELECT 
    'stg_sellers' as table_name,
    COUNT(*) as total_records,
    SUM(CASE WHEN had_duplicates THEN 1 ELSE 0 END) as records_with_duplicates,
    ROUND(SUM(CASE WHEN had_duplicates THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as duplicate_percentage
  FROM `project-olist-470307.dbt_olist_stg.stg_sellers`
)

SELECT 
  table_name,
  total_records,
  records_with_duplicates,
  duplicate_percentage,
  CASE 
    WHEN duplicate_percentage = 0 THEN '✅ No Duplicates'
    WHEN duplicate_percentage < 1 THEN '⚠️ Low Duplicates'
    WHEN duplicate_percentage < 5 THEN '🔶 Medium Duplicates'
    ELSE '🔴 High Duplicates'
  END as duplicate_status
FROM duplicate_stats
ORDER BY duplicate_percentage DESC, total_records DESC;
