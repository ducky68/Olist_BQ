-- Missing ZIP Codes Analysis: stg_customers vs stg_geolocation
-- Identify which ZIP codes exist in one table but not the other

WITH 

customer_zips AS (
  SELECT DISTINCT
    customer_zip_code_prefix as zip_code,
    'CUSTOMERS' as source_table
  FROM `project-olist-470307.dbt_olist_stg.stg_customers`
  WHERE customer_zip_code_prefix IS NOT NULL
    AND LENGTH(CAST(customer_zip_code_prefix AS STRING)) = 5
    AND REGEXP_CONTAINS(CAST(customer_zip_code_prefix AS STRING), r'^[0-9]{5}$')
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

-- Find ZIPs missing in geolocation but present in customers
missing_in_geolocation AS (
  SELECT 
    c.zip_code,
    'MISSING_IN_GEOLOCATION' as discrepancy_type,
    COUNT(*) as customer_count
  FROM customer_zips c
  LEFT JOIN geolocation_zips g ON c.zip_code = g.zip_code
  WHERE g.zip_code IS NULL
  GROUP BY c.zip_code
),

-- Find ZIPs missing in customers but present in geolocation
missing_in_customers AS (
  SELECT 
    g.zip_code,
    'MISSING_IN_CUSTOMERS' as discrepancy_type,
    0 as customer_count  -- No customers for these ZIPs
  FROM geolocation_zips g
  LEFT JOIN customer_zips c ON g.zip_code = c.zip_code
  WHERE c.zip_code IS NULL
),

-- Get sample locations for context
customer_locations AS (
  SELECT DISTINCT
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    COUNT(*) as count_customers
  FROM `project-olist-470307.dbt_olist_stg.stg_customers`
  WHERE customer_zip_code_prefix IS NOT NULL
    AND LENGTH(CAST(customer_zip_code_prefix AS STRING)) = 5
    AND REGEXP_CONTAINS(CAST(customer_zip_code_prefix AS STRING), r'^[0-9]{5}$')
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
  SUM(customer_count) as affected_customers
FROM (
  SELECT zip_code, discrepancy_type, customer_count FROM missing_in_geolocation
  UNION ALL
  SELECT zip_code, discrepancy_type, customer_count FROM missing_in_customers
) combined
GROUP BY discrepancy_type

UNION ALL

-- Detailed list of ZIPs missing in geolocation (affects customers)
SELECT 
  'DETAIL' as section,
  CONCAT('ZIP_', m.zip_code, '_MISSING_IN_GEOLOCATION') as discrepancy_type,
  1 as missing_zip_count,
  m.customer_count as affected_customers
FROM missing_in_geolocation m
JOIN customer_locations c ON m.zip_code = c.customer_zip_code_prefix

UNION ALL

-- Sample of ZIPs missing in customers (exist in geolocation but no customers use them)
SELECT 
  'DETAIL' as section,
  CONCAT('ZIP_', m.zip_code, '_MISSING_IN_CUSTOMERS') as discrepancy_type,
  1 as missing_zip_count,
  0 as affected_customers
FROM missing_in_customers m

ORDER BY 
  CASE section 
    WHEN 'SUMMARY' THEN 1
    WHEN 'DETAIL' THEN 2
  END,
  affected_customers DESC;

-- Separate query for detailed location information
-- Run this separately to see specific city/state info for missing ZIPs:

/*
SELECT 
  'ZIPs in CUSTOMERS but NOT in GEOLOCATION' as issue_type,
  c.customer_zip_code_prefix as zip_code,
  c.customer_city,
  c.customer_state,
  c.count_customers
FROM customer_locations c
LEFT JOIN geolocation_locations g ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
WHERE g.geolocation_zip_code_prefix IS NULL
  AND LENGTH(CAST(c.customer_zip_code_prefix AS STRING)) = 5
  AND REGEXP_CONTAINS(CAST(c.customer_zip_code_prefix AS STRING), r'^[0-9]{5}$')
ORDER BY c.count_customers DESC
LIMIT 50;

SELECT 
  'ZIPs in GEOLOCATION but NOT in CUSTOMERS' as issue_type,
  g.geolocation_zip_code_prefix as zip_code,
  g.geolocation_city,
  g.geolocation_state,
  0 as count_customers
FROM geolocation_locations g
LEFT JOIN customer_locations c ON g.geolocation_zip_code_prefix = c.customer_zip_code_prefix
WHERE c.customer_zip_code_prefix IS NULL
  AND LENGTH(CAST(g.geolocation_zip_code_prefix AS STRING)) = 5
  AND REGEXP_CONTAINS(CAST(g.geolocation_zip_code_prefix AS STRING), r'^[0-9]{5}$')
ORDER BY g.geolocation_zip_code_prefix
LIMIT 50;
*/
