-- Data Quality Check: stg_products NULL handling verification
-- Check for 'unknown' values, NULL values, and '0' values in specific columns

WITH product_quality_check AS (
  SELECT 
    -- Check product_category_name for 'unknown' values
    COUNTIF(product_category_name = 'unknown') as category_unknown_count,
    COUNTIF(product_category_name IS NULL) as category_null_count,
    
    -- Check product_name_length for -1 and NULL values
    COUNTIF(product_name_length = -1) as name_length_negative_one_count,
    COUNTIF(product_name_length IS NULL) as name_length_null_count,
    
    -- Check product_description_length for -1 and NULL values
    COUNTIF(product_description_length = -1) as desc_length_negative_one_count,
    COUNTIF(product_description_length IS NULL) as desc_length_null_count,
    
    -- Check product_photos_qty for 0 and NULL values
    COUNTIF(product_photos_qty = 0) as photos_qty_zero_count,
    COUNTIF(product_photos_qty IS NULL) as photos_qty_null_count,
    
    -- Check dimensions for NULL values (should remain NULL)
    COUNTIF(product_weight_g IS NULL) as weight_null_count,
    COUNTIF(product_length_cm IS NULL) as length_null_count,
    COUNTIF(product_height_cm IS NULL) as height_null_count,
    COUNTIF(product_width_cm IS NULL) as width_null_count,
    
    -- Total record count
    COUNT(*) as total_records
    
  FROM `project-olist-470307.dbt_olist_stg.stg_products`
)

SELECT 
  '📊 PRODUCT DATA QUALITY SUMMARY' as section,
  CONCAT(
    'Total Records: ', total_records, ' | ',
    'Category "unknown": ', category_unknown_count, ' | ',
    'Category NULL: ', category_null_count, ' | ',
    'Name Length -1: ', name_length_negative_one_count, ' | ',
    'Name Length NULL: ', name_length_null_count, ' | ',
    'Desc Length -1: ', desc_length_negative_one_count, ' | ',
    'Desc Length NULL: ', desc_length_null_count, ' | ',
    'Photos Qty 0: ', photos_qty_zero_count, ' | ',
    'Photos Qty NULL: ', photos_qty_null_count, ' | ',
    'Weight NULL: ', weight_null_count, ' | ',
    'Length NULL: ', length_null_count, ' | ',
    'Height NULL: ', height_null_count, ' | ',
    'Width NULL: ', width_null_count
  ) as summary_details
FROM product_quality_check

UNION ALL

-- Detailed breakdown
SELECT 
  '🔍 DETAILED BREAKDOWN' as section,
  'Category Name Analysis' as summary_details
FROM product_quality_check

UNION ALL

SELECT 
  '📝 Category Name' as section,
  CONCAT('Unknown values: ', category_unknown_count, ' (', 
         ROUND(category_unknown_count / total_records * 100, 2), '%)') as summary_details
FROM product_quality_check

UNION ALL

SELECT 
  '📝 Product Name Length' as section,
  CONCAT('Values set to -1: ', name_length_negative_one_count, ' (', 
         ROUND(name_length_negative_one_count / total_records * 100, 2), '%)') as summary_details
FROM product_quality_check

UNION ALL

SELECT 
  '📝 Product Description Length' as section,
  CONCAT('Values set to -1: ', desc_length_negative_one_count, ' (', 
         ROUND(desc_length_negative_one_count / total_records * 100, 2), '%)') as summary_details
FROM product_quality_check

UNION ALL

SELECT 
  '📝 Product Photos Quantity' as section,
  CONCAT('Values set to 0: ', photos_qty_zero_count, ' (', 
         ROUND(photos_qty_zero_count / total_records * 100, 2), '%)') as summary_details
FROM product_quality_check

UNION ALL

SELECT 
  '📝 Dimension NULLs (Expected)' as section,
  CONCAT('Weight: ', weight_null_count, ', Length: ', length_null_count, 
         ', Height: ', height_null_count, ', Width: ', width_null_count) as summary_details
FROM product_quality_check

ORDER BY 
  CASE section 
    WHEN '📊 PRODUCT DATA QUALITY SUMMARY' THEN 1
    WHEN '🔍 DETAILED BREAKDOWN' THEN 2
    WHEN '📝 Category Name' THEN 3
    WHEN '📝 Product Name Length' THEN 4
    WHEN '📝 Product Description Length' THEN 5
    WHEN '📝 Product Photos Quantity' THEN 6
    WHEN '📝 Dimension NULLs (Expected)' THEN 7
  END;