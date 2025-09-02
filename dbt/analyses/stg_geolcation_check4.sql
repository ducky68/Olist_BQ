-- Best Practice Validation Query for Brazilian Geolocation Data
-- Comprehensive checks for geolocation_zip_code_prefix, geolocation_city, geolocation_state

WITH validation_checks AS (
  SELECT 
    geolocation_zip_code_prefix,
    geolocation_city,
    geolocation_state,
    geolocation_lat,
    geolocation_lng,
    
    -- ZIP CODE VALIDATIONS
    CASE 
      WHEN geolocation_zip_code_prefix IS NULL THEN 'ZIP_NULL'
      WHEN LENGTH(TRIM(geolocation_zip_code_prefix)) != 5 THEN 'ZIP_WRONG_LENGTH'
      WHEN NOT REGEXP_CONTAINS(geolocation_zip_code_prefix, r'^[0-9]{5}$') THEN 'ZIP_NOT_NUMERIC'
      WHEN CAST(geolocation_zip_code_prefix AS INT64) < 1000 THEN 'ZIP_TOO_LOW'
      WHEN CAST(geolocation_zip_code_prefix AS INT64) > 99999 THEN 'ZIP_TOO_HIGH'
      ELSE 'ZIP_VALID'
    END as zip_validation,
    
    -- CITY VALIDATIONS
    CASE 
      WHEN geolocation_city IS NULL THEN 'CITY_NULL'
      WHEN LENGTH(TRIM(geolocation_city)) = 0 THEN 'CITY_EMPTY'
      WHEN LENGTH(TRIM(geolocation_city)) < 2 THEN 'CITY_TOO_SHORT'
      WHEN LENGTH(TRIM(geolocation_city)) > 50 THEN 'CITY_TOO_LONG'
      WHEN REGEXP_CONTAINS(geolocation_city, r'[0-9]') THEN 'CITY_CONTAINS_NUMBERS'
      WHEN REGEXP_CONTAINS(geolocation_city, r'[^a-zA-Z\s\-\'àáâãäçèéêëìíîïñòóôõöùúûüýÿ]') THEN 'CITY_INVALID_CHARS'
      WHEN REGEXP_CONTAINS(geolocation_city, r'^[^a-zA-Zàáâãäçèéêëìíîïñòóôõöùúûüýÿ]') THEN 'CITY_STARTS_INVALID'
      ELSE 'CITY_VALID'
    END as city_validation,
    
    -- STATE VALIDATIONS (Brazilian states)
    CASE 
      WHEN geolocation_state IS NULL THEN 'STATE_NULL'
      WHEN LENGTH(TRIM(geolocation_state)) != 2 THEN 'STATE_WRONG_LENGTH'
      WHEN NOT REGEXP_CONTAINS(geolocation_state, r'^[A-Z]{2}$') THEN 'STATE_INVALID_FORMAT'
      WHEN geolocation_state NOT IN (
        'SP', 'RJ', 'MG', 'RS', 'PR', 'SC', 'BA', 'GO', 'ES', 'PE', 'CE', 'PB', 
        'PA', 'RN', 'AL', 'MT', 'MS', 'DF', 'PI', 'SE', 'RO', 'TO', 'AC', 'AM', 'AP', 'RR'
      ) THEN 'STATE_NOT_BRAZILIAN'
      ELSE 'STATE_VALID'
    END as state_validation,
    
    -- COORDINATE VALIDATIONS (Brazil bounds)
    CASE 
      WHEN geolocation_lat IS NULL THEN 'LAT_NULL'
      WHEN geolocation_lat < -35 OR geolocation_lat > 5 THEN 'LAT_OUT_OF_BRAZIL_BOUNDS'
      ELSE 'LAT_VALID'
    END as lat_validation,
    
    CASE 
      WHEN geolocation_lng IS NULL THEN 'LNG_NULL'
      WHEN geolocation_lng < -75 OR geolocation_lng > -30 THEN 'LNG_OUT_OF_BRAZIL_BOUNDS'
      ELSE 'LNG_VALID'
    END as lng_validation,
    
    -- LOGICAL CONSISTENCY CHECKS
    -- Check if city name matches expected patterns for the state
    CASE 
      WHEN geolocation_state = 'SP' AND LOWER(geolocation_city) LIKE '%rio%de%janeiro%' THEN 'CITY_STATE_MISMATCH'
      WHEN geolocation_state = 'RJ' AND LOWER(geolocation_city) LIKE '%sao%paulo%' THEN 'CITY_STATE_MISMATCH'
      WHEN geolocation_state = 'DF' AND LOWER(geolocation_city) NOT LIKE '%brasilia%' AND LOWER(geolocation_city) NOT LIKE '%brasília%' THEN 'CITY_STATE_QUESTIONABLE'
      ELSE 'CITY_STATE_CONSISTENT'
    END as consistency_check
    
  FROM `project-olist-470307.dbt_olist_stg.stg_geolocation`
),

-- Validation summary by issue type
validation_summary AS (
  SELECT 
    'ZIP_CODE_ISSUES' as category,
    zip_validation as issue_type,
    COUNT(*) as record_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM validation_checks), 2) as percentage
  FROM validation_checks
  WHERE zip_validation != 'ZIP_VALID'
  GROUP BY zip_validation
  
  UNION ALL
  
  SELECT 
    'CITY_ISSUES' as category,
    city_validation as issue_type,
    COUNT(*) as record_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM validation_checks), 2) as percentage
  FROM validation_checks
  WHERE city_validation != 'CITY_VALID'
  GROUP BY city_validation
  
  UNION ALL
  
  SELECT 
    'STATE_ISSUES' as category,
    state_validation as issue_type,
    COUNT(*) as record_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM validation_checks), 2) as percentage
  FROM validation_checks
  WHERE state_validation != 'STATE_VALID'
  GROUP BY state_validation
  
  UNION ALL
  
  SELECT 
    'COORDINATE_ISSUES' as category,
    CONCAT(lat_validation, '_OR_', lng_validation) as issue_type,
    COUNT(*) as record_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM validation_checks), 2) as percentage
  FROM validation_checks
  WHERE lat_validation != 'LAT_VALID' OR lng_validation != 'LNG_VALID'
  GROUP BY lat_validation, lng_validation
  
  UNION ALL
  
  SELECT 
    'CONSISTENCY_ISSUES' as category,
    consistency_check as issue_type,
    COUNT(*) as record_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM validation_checks), 2) as percentage
  FROM validation_checks
  WHERE consistency_check != 'CITY_STATE_CONSISTENT'
  GROUP BY consistency_check
),

-- Overall data quality score
overall_quality AS (
  SELECT 
    COUNT(*) as total_records,
    SUM(CASE WHEN zip_validation = 'ZIP_VALID' 
             AND city_validation = 'CITY_VALID' 
             AND state_validation = 'STATE_VALID' 
             AND lat_validation = 'LAT_VALID' 
             AND lng_validation = 'LNG_VALID' 
             AND consistency_check = 'CITY_STATE_CONSISTENT' 
        THEN 1 ELSE 0 END) as fully_valid_records,
    ROUND(SUM(CASE WHEN zip_validation = 'ZIP_VALID' 
                   AND city_validation = 'CITY_VALID' 
                   AND state_validation = 'STATE_VALID' 
                   AND lat_validation = 'LAT_VALID' 
                   AND lng_validation = 'LNG_VALID' 
                   AND consistency_check = 'CITY_STATE_CONSISTENT' 
              THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as data_quality_score
  FROM validation_checks
)

-- Results
SELECT 
  'OVERALL_QUALITY' as section,
  'Data Quality Score' as metric,
  CONCAT(CAST(data_quality_score AS STRING), '% (', 
         CAST(fully_valid_records AS STRING), ' of ', 
         CAST(total_records AS STRING), ' records)') as value,
  'Percentage of records passing all validation checks' as description
FROM overall_quality

UNION ALL

SELECT 
  'VALIDATION_ISSUES' as section,
  CONCAT(category, ': ', issue_type) as metric,
  CONCAT(CAST(record_count AS STRING), ' (', CAST(percentage AS STRING), '%)') as value,
  'Records with validation issues' as description
FROM validation_summary

ORDER BY section, value DESC;

-- Additional queries for deeper analysis (uncomment as needed)

-- Top 20 most problematic records
/*
SELECT 
  geolocation_zip_code_prefix,
  geolocation_city,
  geolocation_state,
  zip_validation,
  city_validation,
  state_validation,
  consistency_check
FROM validation_checks
WHERE zip_validation != 'ZIP_VALID' 
   OR city_validation != 'CITY_VALID'
   OR state_validation != 'STATE_VALID'
   OR consistency_check != 'CITY_STATE_CONSISTENT'
ORDER BY 
  CASE WHEN state_validation != 'STATE_VALID' THEN 1 ELSE 2 END,
  CASE WHEN zip_validation != 'ZIP_VALID' THEN 1 ELSE 2 END,
  geolocation_state, geolocation_city
LIMIT 20;
*/

-- State-by-state validation summary
/*
SELECT 
  geolocation_state,
  COUNT(*) as total_records,
  SUM(CASE WHEN zip_validation = 'ZIP_VALID' THEN 1 ELSE 0 END) as valid_zips,
  SUM(CASE WHEN city_validation = 'CITY_VALID' THEN 1 ELSE 0 END) as valid_cities,
  ROUND(SUM(CASE WHEN zip_validation = 'ZIP_VALID' AND city_validation = 'CITY_VALID' 
                 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as state_quality_score
FROM validation_checks
WHERE geolocation_state IS NOT NULL
GROUP BY geolocation_state
ORDER BY state_quality_score DESC, total_records DESC;
*/
