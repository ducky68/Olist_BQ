-- Null Value Analysis Across All Staging Tables
-- This query checks for null values in every column of every staging table
-- Run this in BigQuery Console to get a comprehensive null value report

WITH null_checks AS (
  -- STG_CUSTOMERS
  SELECT 
    'stg_customers' as table_name,
    'customer_id' as column_name,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_customers`
  
  UNION ALL
  
  SELECT 
    'stg_customers' as table_name,
    'customer_unique_id' as column_name,
    SUM(CASE WHEN customer_unique_id IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN customer_unique_id IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_customers`
  
  UNION ALL
  
  SELECT 
    'stg_customers' as table_name,
    'customer_zip_code_prefix' as column_name,
    SUM(CASE WHEN customer_zip_code_prefix IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN customer_zip_code_prefix IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_customers`
  
  UNION ALL
  
  SELECT 
    'stg_customers' as table_name,
    'customer_city' as column_name,
    SUM(CASE WHEN customer_city IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN customer_city IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_customers`
  
  UNION ALL
  
  SELECT 
    'stg_customers' as table_name,
    'customer_state' as column_name,
    SUM(CASE WHEN customer_state IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN customer_state IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_customers`

  UNION ALL

  -- STG_GEOLOCATION
  SELECT 
    'stg_geolocation' as table_name,
    'geolocation_zip_code_prefix' as column_name,
    SUM(CASE WHEN geolocation_zip_code_prefix IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN geolocation_zip_code_prefix IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_geolocation`
  
  UNION ALL
  
  SELECT 
    'stg_geolocation' as table_name,
    'geolocation_lat' as column_name,
    SUM(CASE WHEN geolocation_lat IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN geolocation_lat IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_geolocation`
  
  UNION ALL
  
  SELECT 
    'stg_geolocation' as table_name,
    'geolocation_lng' as column_name,
    SUM(CASE WHEN geolocation_lng IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN geolocation_lng IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_geolocation`
  
  UNION ALL
  
  SELECT 
    'stg_geolocation' as table_name,
    'geolocation_city' as column_name,
    SUM(CASE WHEN geolocation_city IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN geolocation_city IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_geolocation`
  
  UNION ALL
  
  SELECT 
    'stg_geolocation' as table_name,
    'geolocation_state' as column_name,
    SUM(CASE WHEN geolocation_state IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN geolocation_state IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_geolocation`

  UNION ALL

  -- STG_ORDER_ITEMS
  SELECT 
    'stg_order_items' as table_name,
    'order_id' as column_name,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_order_items`
  
  UNION ALL
  
  SELECT 
    'stg_order_items' as table_name,
    'order_item_id' as column_name,
    SUM(CASE WHEN order_item_id IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN order_item_id IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_order_items`
  
  UNION ALL
  
  SELECT 
    'stg_order_items' as table_name,
    'product_id' as column_name,
    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_order_items`
  
  UNION ALL
  
  SELECT 
    'stg_order_items' as table_name,
    'seller_id' as column_name,
    SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_order_items`
  
  UNION ALL
  
  SELECT 
    'stg_order_items' as table_name,
    'shipping_limit_date' as column_name,
    SUM(CASE WHEN shipping_limit_date IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN shipping_limit_date IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_order_items`
  
  UNION ALL
  
  SELECT 
    'stg_order_items' as table_name,
    'price' as column_name,
    SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_order_items`
  
  UNION ALL
  
  SELECT 
    'stg_order_items' as table_name,
    'freight_value' as column_name,
    SUM(CASE WHEN freight_value IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN freight_value IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_order_items`

  UNION ALL

  -- STG_ORDER_PAYMENTS
  SELECT 
    'stg_order_payments' as table_name,
    'order_id' as column_name,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_order_payments`
  
  UNION ALL
  
  SELECT 
    'stg_order_payments' as table_name,
    'payment_sequential' as column_name,
    SUM(CASE WHEN payment_sequential IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN payment_sequential IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_order_payments`
  
  UNION ALL
  
  SELECT 
    'stg_order_payments' as table_name,
    'payment_type' as column_name,
    SUM(CASE WHEN payment_type IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN payment_type IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_order_payments`
  
  UNION ALL
  
  SELECT 
    'stg_order_payments' as table_name,
    'payment_installments' as column_name,
    SUM(CASE WHEN payment_installments IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN payment_installments IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_order_payments`
  
  UNION ALL
  
  SELECT 
    'stg_order_payments' as table_name,
    'payment_value' as column_name,
    SUM(CASE WHEN payment_value IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN payment_value IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_order_payments`

  UNION ALL

  -- STG_ORDER_REVIEWS
  SELECT 
    'stg_order_reviews' as table_name,
    'review_id' as column_name,
    SUM(CASE WHEN review_id IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN review_id IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_order_reviews`
  
  UNION ALL
  
  SELECT 
    'stg_order_reviews' as table_name,
    'order_id' as column_name,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_order_reviews`
  
  UNION ALL
  
  SELECT 
    'stg_order_reviews' as table_name,
    'review_score' as column_name,
    SUM(CASE WHEN review_score IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN review_score IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_order_reviews`
  
  UNION ALL
  
  SELECT 
    'stg_order_reviews' as table_name,
    'review_comment_title' as column_name,
    SUM(CASE WHEN review_comment_title IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN review_comment_title IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_order_reviews`
  
  UNION ALL
  
  SELECT 
    'stg_order_reviews' as table_name,
    'review_comment_message' as column_name,
    SUM(CASE WHEN review_comment_message IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN review_comment_message IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_order_reviews`
  
  UNION ALL
  
  SELECT 
    'stg_order_reviews' as table_name,
    'review_creation_date' as column_name,
    SUM(CASE WHEN review_creation_date IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN review_creation_date IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_order_reviews`
  
  UNION ALL
  
  SELECT 
    'stg_order_reviews' as table_name,
    'review_answer_timestamp' as column_name,
    SUM(CASE WHEN review_answer_timestamp IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN review_answer_timestamp IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_order_reviews`

  UNION ALL

  -- STG_ORDERS
  SELECT 
    'stg_orders' as table_name,
    'order_id' as column_name,
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_orders`
  
  UNION ALL
  
  SELECT 
    'stg_orders' as table_name,
    'customer_id' as column_name,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_orders`
  
  UNION ALL
  
  SELECT 
    'stg_orders' as table_name,
    'order_status' as column_name,
    SUM(CASE WHEN order_status IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN order_status IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_orders`
  
  UNION ALL
  
  SELECT 
    'stg_orders' as table_name,
    'order_purchase_timestamp' as column_name,
    SUM(CASE WHEN order_purchase_timestamp IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN order_purchase_timestamp IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_orders`
  
  UNION ALL
  
  SELECT 
    'stg_orders' as table_name,
    'order_approved_at' as column_name,
    SUM(CASE WHEN order_approved_at IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN order_approved_at IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_orders`
  
  UNION ALL
  
  SELECT 
    'stg_orders' as table_name,
    'order_delivered_carrier_date' as column_name,
    SUM(CASE WHEN order_delivered_carrier_date IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN order_delivered_carrier_date IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_orders`
  
  UNION ALL
  
  SELECT 
    'stg_orders' as table_name,
    'order_delivered_customer_date' as column_name,
    SUM(CASE WHEN order_delivered_customer_date IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN order_delivered_customer_date IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_orders`
  
  UNION ALL
  
  SELECT 
    'stg_orders' as table_name,
    'order_estimated_delivery_date' as column_name,
    SUM(CASE WHEN order_estimated_delivery_date IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN order_estimated_delivery_date IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_orders`

  UNION ALL

  -- STG_PRODUCT_CATEGORY_NAME_TRANSLATION
  SELECT 
    'stg_product_category_name_translation' as table_name,
    'product_category_name' as column_name,
    SUM(CASE WHEN product_category_name IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN product_category_name IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_product_category_name_translation`
  
  UNION ALL
  
  SELECT 
    'stg_product_category_name_translation' as table_name,
    'product_category_name_english' as column_name,
    SUM(CASE WHEN product_category_name_english IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN product_category_name_english IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_product_category_name_translation`

  UNION ALL

  -- STG_PRODUCTS
  SELECT 
    'stg_products' as table_name,
    'product_id' as column_name,
    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_products`
  
  UNION ALL
  
  SELECT 
    'stg_products' as table_name,
    'product_category_name' as column_name,
    SUM(CASE WHEN product_category_name IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN product_category_name IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_products`
  
  UNION ALL
  
  SELECT 
    'stg_products' as table_name,
    'product_name_length' as column_name,
    SUM(CASE WHEN product_name_length IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN product_name_length IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_products`
  
  UNION ALL
  
  SELECT 
    'stg_products' as table_name,
    'product_description_length' as column_name,
    SUM(CASE WHEN product_description_length IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN product_description_length IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_products`
  
  UNION ALL
  
  SELECT 
    'stg_products' as table_name,
    'product_photos_qty' as column_name,
    SUM(CASE WHEN product_photos_qty IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN product_photos_qty IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_products`
  
  UNION ALL
  
  SELECT 
    'stg_products' as table_name,
    'product_weight_g' as column_name,
    SUM(CASE WHEN product_weight_g IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN product_weight_g IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_products`
  
  UNION ALL
  
  SELECT 
    'stg_products' as table_name,
    'product_length_cm' as column_name,
    SUM(CASE WHEN product_length_cm IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN product_length_cm IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_products`
  
  UNION ALL
  
  SELECT 
    'stg_products' as table_name,
    'product_height_cm' as column_name,
    SUM(CASE WHEN product_height_cm IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN product_height_cm IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_products`
  
  UNION ALL
  
  SELECT 
    'stg_products' as table_name,
    'product_width_cm' as column_name,
    SUM(CASE WHEN product_width_cm IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN product_width_cm IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_products`

  UNION ALL

  -- STG_SELLERS
  SELECT 
    'stg_sellers' as table_name,
    'seller_id' as column_name,
    SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_sellers`
  
  UNION ALL
  
  SELECT 
    'stg_sellers' as table_name,
    'seller_zip_code_prefix' as column_name,
    SUM(CASE WHEN seller_zip_code_prefix IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN seller_zip_code_prefix IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_sellers`
  
  UNION ALL
  
  SELECT 
    'stg_sellers' as table_name,
    'seller_city' as column_name,
    SUM(CASE WHEN seller_city IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN seller_city IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_sellers`
  
  UNION ALL
  
  SELECT 
    'stg_sellers' as table_name,
    'seller_state' as column_name,
    SUM(CASE WHEN seller_state IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN seller_state IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_olist_stg.stg_sellers`
)

SELECT 
  table_name,
  column_name,
  null_count,
  total_count,
  null_percentage,
  CASE 
    WHEN null_percentage = 0 THEN '✅ No Nulls'
    WHEN null_percentage < 1 THEN '⚠️ Low Nulls'
    WHEN null_percentage < 10 THEN '🔶 Medium Nulls'
    WHEN null_percentage < 50 THEN '🔴 High Nulls'
    ELSE '❌ Mostly Null'
  END as null_status
FROM null_checks
-- Remove the WHERE filter to show all columns, even those with 0 nulls
-- WHERE null_count > 0  -- Only show columns with null values
ORDER BY table_name, null_percentage DESC, column_name;
