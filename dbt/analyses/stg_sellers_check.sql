-- Seller Location vs Geolocation Truth Table Mapping
-- Identify discrepancies between seller locations and geolocation data

WITH 

seller_data AS (
  SELECT DISTINCT
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    COUNT(*) as seller_count
  FROM `project-olist-470307.dbt_olist_stg.stg_sellers`
  WHERE seller_zip_code_prefix IS NOT NULL
  GROUP BY 1, 2, 3
),

geolocation_truth AS (
  SELECT DISTINCT
    geolocation_zip_code_prefix,
    geolocation_city,
    geolocation_state
  FROM `project-olist-470307.dbt_olist_stg.stg_geolocation`
  WHERE geolocation_zip_code_prefix IS NOT NULL
),

mapping_analysis AS (
  SELECT 
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,
    s.seller_count,
    g.geolocation_city,
    g.geolocation_state,
    
    CASE 
      WHEN g.geolocation_zip_code_prefix IS NULL THEN 'ZIP_NOT_FOUND'
      WHEN s.seller_city = g.geolocation_city AND s.seller_state = g.geolocation_state THEN 'PERFECT_MATCH'
      WHEN s.seller_city != g.geolocation_city AND s.seller_state = g.geolocation_state THEN 'CITY_MISMATCH'
      WHEN s.seller_city = g.geolocation_city AND s.seller_state != g.geolocation_state THEN 'STATE_MISMATCH'
      ELSE 'BOTH_MISMATCH'
    END AS discrepancy_type
    
  FROM seller_data s
  LEFT JOIN geolocation_truth g
    ON s.seller_zip_code_prefix = g.geolocation_zip_code_prefix
)

SELECT 
  discrepancy_type,
  COUNT(*) as unique_zip_codes,
  SUM(seller_count) as total_sellers_affected,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM mapping_analysis), 2) as percentage_of_zips
FROM mapping_analysis
GROUP BY discrepancy_type

UNION ALL

SELECT 
  'TOTAL' as discrepancy_type,
  COUNT(*) as unique_zip_codes,
  SUM(seller_count) as total_sellers_affected,
  100.0 as percentage_of_zips
FROM mapping_analysis

ORDER BY 
  CASE discrepancy_type
    WHEN 'PERFECT_MATCH' THEN 1
    WHEN 'CITY_MISMATCH' THEN 2
    WHEN 'STATE_MISMATCH' THEN 3
    WHEN 'BOTH_MISMATCH' THEN 4
    WHEN 'ZIP_NOT_FOUND' THEN 5
    WHEN 'TOTAL' THEN 6
  END;

-- Detailed view of discrepancies
SELECT 
  '=== DETAILED DISCREPANCIES ===' as section,
  '' as zip_code,
  '' as seller_location,
  '' as truth_location,
  '' as discrepancy_type,
  0 as seller_count

UNION ALL

SELECT 
  'CITY_MISMATCH' as section,
  seller_zip_code_prefix as zip_code,
  CONCAT(seller_city, ', ', seller_state) as seller_location,
  CONCAT(geolocation_city, ', ', geolocation_state) as truth_location,
  discrepancy_type,
  seller_count
FROM mapping_analysis
WHERE discrepancy_type = 'CITY_MISMATCH'
ORDER BY seller_count DESC
LIMIT 10

UNION ALL

SELECT 
  'STATE_MISMATCH' as section,
  seller_zip_code_prefix as zip_code,
  CONCAT(seller_city, ', ', seller_state) as seller_location,
  CONCAT(geolocation_city, ', ', geolocation_state) as truth_location,
  discrepancy_type,
  seller_count
FROM mapping_analysis
WHERE discrepancy_type = 'STATE_MISMATCH'
ORDER BY seller_count DESC
LIMIT 10

UNION ALL

SELECT 
  'BOTH_MISMATCH' as section,
  seller_zip_code_prefix as zip_code,
  CONCAT(seller_city, ', ', seller_state) as seller_location,
  CONCAT(geolocation_city, ', ', geolocation_state) as truth_location,
  discrepancy_type,
  seller_count
FROM mapping_analysis
WHERE discrepancy_type = 'BOTH_MISMATCH'
ORDER BY seller_count DESC
LIMIT 10

UNION ALL

SELECT 
  'ZIP_NOT_FOUND' as section,
  seller_zip_code_prefix as zip_code,
  CONCAT(seller_city, ', ', seller_state) as seller_location,
  'NOT_FOUND' as truth_location,
  discrepancy_type,
  seller_count
FROM mapping_analysis
WHERE discrepancy_type = 'ZIP_NOT_FOUND'
ORDER BY seller_count DESC
LIMIT 10

ORDER BY 
  CASE section
    WHEN '=== DETAILED DISCREPANCIES ===' THEN 1
    WHEN 'CITY_MISMATCH' THEN 2
    WHEN 'STATE_MISMATCH' THEN 3
    WHEN 'BOTH_MISMATCH' THEN 4
    WHEN 'ZIP_NOT_FOUND' THEN 5
  END,
  seller_count DESC;