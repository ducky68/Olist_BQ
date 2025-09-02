-- Missing ZIP Codes Analysis: stg_sellers vs stg_geolocation
-- Identify which ZIP codes exist in one table but not the other

WITH 

seller_zips AS (
  SELECT DISTINCT
    seller_zip_code_prefix as zip_code,
    'SELLERS' as source_table
  FROM `project-olist-470307.dbt_olist_stg.stg_sellers`
  WHERE seller_zip_code_prefix IS NOT NULL
    AND LENGTH(CAST(seller_zip_code_prefix AS STRING)) = 5
    AND REGEXP_CONTAINS(CAST(seller_zip_code_prefix AS STRING), r'^[0-9]{5}$')
),

geolocation_zips AS (
  SELECT DISTINCT
    geolocation_zip_code_prefix as zip_code,
    'GEOLOCATION' as source_table
  FROM `project-olist-470307.dbt_olist_stg.stg_geolocation`
  WHERE geolocation_zip_code_prefix IS NOT NULL
    AND LENGTH(CAST(geolocation_zip_code_prefix AS STRING)) = 5
    AND REGEXP_CONTAINS(CAST(geolocation_zip_code_prefix AS STRING), r'^[0-9]{5}$')
),

-- Find ZIPs missing in geolocation but present in sellers
missing_in_geolocation AS (
  SELECT 
    s.zip_code,
    'MISSING_IN_GEOLOCATION' as discrepancy_type,
    COUNT(*) as seller_count
  FROM seller_zips s
  LEFT JOIN geolocation_zips g ON s.zip_code = g.zip_code
  WHERE g.zip_code IS NULL
  GROUP BY s.zip_code
),

-- Find ZIPs missing in sellers but present in geolocation
missing_in_sellers AS (
  SELECT 
    g.zip_code,
    'MISSING_IN_SELLERS' as discrepancy_type,
    0 as seller_count  -- No sellers for these ZIPs
  FROM geolocation_zips g
  LEFT JOIN seller_zips s ON g.zip_code = s.zip_code
  WHERE s.zip_code IS NULL
),

-- Get sample locations for context
seller_locations AS (
  SELECT DISTINCT
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    COUNT(*) as count_sellers
  FROM `project-olist-470307.dbt_olist_stg.stg_sellers`
  WHERE seller_zip_code_prefix IS NOT NULL
    AND LENGTH(CAST(seller_zip_code_prefix AS STRING)) = 5
    AND REGEXP_CONTAINS(CAST(seller_zip_code_prefix AS STRING), r'^[0-9]{5}$')
  GROUP BY 1, 2, 3
),

geolocation_locations AS (
  SELECT DISTINCT
    geolocation_zip_code_prefix,
    geolocation_city,
    geolocation_state
  FROM `project-olist-470307.dbt_olist_stg.stg_geolocation`
  WHERE geolocation_zip_code_prefix IS NOT NULL
    AND LENGTH(CAST(geolocation_zip_code_prefix AS STRING)) = 5
    AND REGEXP_CONTAINS(CAST(geolocation_zip_code_prefix AS STRING), r'^[0-9]{5}$')
)

-- Summary of missing ZIP codes
SELECT 
  'SUMMARY' as section,
  discrepancy_type,
  COUNT(*) as missing_zip_count,
  SUM(seller_count) as affected_sellers
FROM (
  SELECT zip_code, discrepancy_type, seller_count FROM missing_in_geolocation
  UNION ALL
  SELECT zip_code, discrepancy_type, seller_count FROM missing_in_sellers
) combined
GROUP BY discrepancy_type

UNION ALL

-- Detailed list of ZIPs missing in geolocation (affects sellers)
SELECT 
  'DETAIL' as section,
  CONCAT('ZIP_', m.zip_code, '_MISSING_IN_GEOLOCATION') as discrepancy_type,
  1 as missing_zip_count,
  m.seller_count as affected_sellers
FROM missing_in_geolocation m
JOIN seller_locations s ON m.zip_code = s.seller_zip_code_prefix

UNION ALL

-- Sample of ZIPs missing in sellers (exist in geolocation but no sellers use them)
SELECT 
  'DETAIL' as section,
  CONCAT('ZIP_', m.zip_code, '_MISSING_IN_SELLERS') as discrepancy_type,
  1 as missing_zip_count,
  0 as affected_sellers
FROM missing_in_sellers m

ORDER BY 
  CASE section 
    WHEN 'SUMMARY' THEN 1
    WHEN 'DETAIL' THEN 2
  END,
  affected_sellers DESC;

-- Separate query for detailed location information
-- Run this separately to see specific city/state info for missing ZIPs:

/*
SELECT 
  'ZIPs in SELLERS but NOT in GEOLOCATION' as issue_type,
  s.seller_zip_code_prefix as zip_code,
  s.seller_city,
  s.seller_state,
  s.count_sellers
FROM seller_locations s
LEFT JOIN geolocation_locations g ON s.seller_zip_code_prefix = g.geolocation_zip_code_prefix
WHERE g.geolocation_zip_code_prefix IS NULL
  AND LENGTH(CAST(s.seller_zip_code_prefix AS STRING)) = 5
  AND REGEXP_CONTAINS(CAST(s.seller_zip_code_prefix AS STRING), r'^[0-9]{5}$')
ORDER BY s.count_sellers DESC
LIMIT 50;

SELECT 
  'ZIPs in GEOLOCATION but NOT in SELLERS' as issue_type,
  g.geolocation_zip_code_prefix as zip_code,
  g.geolocation_city,
  g.geolocation_state,
  0 as count_sellers
FROM geolocation_locations g
LEFT JOIN seller_locations s ON g.geolocation_zip_code_prefix = s.seller_zip_code_prefix
WHERE s.seller_zip_code_prefix IS NULL
  AND LENGTH(CAST(g.geolocation_zip_code_prefix AS STRING)) = 5
  AND REGEXP_CONTAINS(CAST(g.geolocation_zip_code_prefix AS STRING), r'^[0-9]{5}$')
ORDER BY g.geolocation_zip_code_prefix
LIMIT 50;
*/
