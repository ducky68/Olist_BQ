-- Detailed Analysis of Invalid Brazilian State Codes in stg_geolocation
-- Investigates the 315 records with invalid state codes

WITH valid_brazilian_states AS (
  SELECT state FROM (
    SELECT 'SP' as state UNION ALL SELECT 'RJ' UNION ALL SELECT 'MG' UNION ALL 
    SELECT 'RS' UNION ALL SELECT 'PR' UNION ALL SELECT 'SC' UNION ALL SELECT 'BA' UNION ALL 
    SELECT 'GO' UNION ALL SELECT 'ES' UNION ALL SELECT 'PE' UNION ALL SELECT 'CE' UNION ALL 
    SELECT 'PB' UNION ALL SELECT 'PA' UNION ALL SELECT 'RN' UNION ALL SELECT 'AL' UNION ALL 
    SELECT 'MT' UNION ALL SELECT 'MS' UNION ALL SELECT 'DF' UNION ALL SELECT 'PI' UNION ALL 
    SELECT 'SE' UNION ALL SELECT 'RO' UNION ALL SELECT 'TO' UNION ALL SELECT 'AC' UNION ALL 
    SELECT 'AM' UNION ALL SELECT 'AP' UNION ALL SELECT 'RR'
  )
),

invalid_state_records AS (
  SELECT 
    geolocation_zip_code_prefix,
    geolocation_city,
    geolocation_state,
    geolocation_lat,
    geolocation_lng,
    had_duplicates,
    ingestion_timestamp,
    -- Analyze the invalid state value
    LENGTH(TRIM(geolocation_state)) as state_length,
    REGEXP_CONTAINS(geolocation_state, r'^[A-Z]{2}$') as is_two_letter_caps,
    REGEXP_CONTAINS(geolocation_state, r'[0-9]') as contains_numbers,
    REGEXP_CONTAINS(geolocation_state, r'[^A-Za-z0-9]') as contains_special_chars,
    CASE 
      WHEN geolocation_state IS NULL THEN 'NULL_VALUE'
      WHEN LENGTH(TRIM(geolocation_state)) = 0 THEN 'EMPTY_STRING'
      WHEN LENGTH(TRIM(geolocation_state)) != 2 THEN 'WRONG_LENGTH'
      WHEN NOT REGEXP_CONTAINS(geolocation_state, r'^[A-Z]{2}$') THEN 'INVALID_FORMAT'
      ELSE 'UNKNOWN_STATE_CODE'
    END as invalid_reason
  FROM `project-olist-470307.dbt_olist_stg.stg_geolocation`
  WHERE geolocation_state NOT IN (
    SELECT state FROM valid_brazilian_states
  ) OR geolocation_state IS NULL
),

invalid_state_summary AS (
  SELECT 
    invalid_reason,
    COUNT(*) as record_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage,
    STRING_AGG(DISTINCT geolocation_state, ', ' ORDER BY geolocation_state LIMIT 10) as sample_invalid_states
  FROM invalid_state_records
  GROUP BY invalid_reason
),

-- Analyze patterns in invalid states
state_pattern_analysis AS (
  SELECT 
    geolocation_state,
    COUNT(*) as frequency,
    COUNT(DISTINCT geolocation_city) as unique_cities,
    COUNT(DISTINCT geolocation_zip_code_prefix) as unique_zip_codes,
    -- Check if this might be a valid state with formatting issues
    CASE 
      WHEN UPPER(TRIM(geolocation_state)) IN (SELECT state FROM valid_brazilian_states) THEN 'FORMATTING_ISSUE'
      WHEN LENGTH(TRIM(geolocation_state)) = 2 AND REGEXP_CONTAINS(geolocation_state, r'^[A-Za-z]{2}$') THEN 'UNKNOWN_STATE_CODE'
      ELSE 'INVALID_FORMAT'
    END as potential_fix
  FROM invalid_state_records
  WHERE geolocation_state IS NOT NULL AND LENGTH(TRIM(geolocation_state)) > 0
  GROUP BY geolocation_state
),

-- Check if invalid states are from dynamic additions (customers/sellers)
source_analysis AS (
  SELECT 
    CASE WHEN had_duplicates = true THEN 'ESTIMATED_COORDINATES' ELSE 'ORIGINAL_DATA' END as data_source,
    COUNT(*) as invalid_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM invalid_state_records), 2) as percentage_of_invalid
  FROM invalid_state_records
  GROUP BY had_duplicates
),

-- Geographic distribution of invalid states
geographic_patterns AS (
  SELECT 
    geolocation_state,
    geolocation_city,
    COUNT(*) as occurrence_count,
    COUNT(DISTINCT geolocation_zip_code_prefix) as zip_count,
    MIN(geolocation_lat) as min_lat,
    MAX(geolocation_lat) as max_lat,
    MIN(geolocation_lng) as min_lng,
    MAX(geolocation_lng) as max_lng
  FROM invalid_state_records
  WHERE geolocation_state IS NOT NULL 
    AND geolocation_lat IS NOT NULL 
    AND geolocation_lng IS NOT NULL
  GROUP BY geolocation_state, geolocation_city
)

-- Results: Summary of Invalid State Issues
SELECT 
  'INVALID_STATE_SUMMARY' as analysis_section,
  invalid_reason as issue_type,
  CAST(record_count AS STRING) as count_value,
  CAST(percentage AS STRING) as percentage_value,
  sample_invalid_states as examples
FROM invalid_state_summary

UNION ALL

-- Results: Top Invalid State Values (will be limited by ORDER BY in final result)
SELECT 
  'TOP_INVALID_STATES' as analysis_section,
  CONCAT('State: ', geolocation_state) as issue_type,
  CAST(frequency AS STRING) as count_value,
  potential_fix as percentage_value,
  CONCAT('Cities: ', CAST(unique_cities AS STRING), ', ZIPs: ', CAST(unique_zip_codes AS STRING)) as examples
FROM state_pattern_analysis

UNION ALL

-- Results: Source Analysis
SELECT 
  'DATA_SOURCE_ANALYSIS' as analysis_section,
  data_source as issue_type,
  CAST(invalid_count AS STRING) as count_value,
  CAST(percentage_of_invalid AS STRING) as percentage_value,
  'Records with invalid states by data source' as examples
FROM source_analysis

ORDER BY analysis_section, CAST(count_value AS INT64) DESC;

-- Separate detailed query for geographic patterns (uncomment to run separately)
/*
SELECT 
  'GEOGRAPHIC_PATTERNS' as analysis_type,
  geolocation_state,
  geolocation_city,
  occurrence_count,
  zip_count,
  CONCAT('Lat: ', CAST(ROUND(min_lat, 4) AS STRING), ' to ', CAST(ROUND(max_lat, 4) AS STRING)) as lat_range,
  CONCAT('Lng: ', CAST(ROUND(min_lng, 4) AS STRING), ' to ', CAST(ROUND(max_lng, 4) AS STRING)) as lng_range
FROM geographic_patterns;
*/

-- Query to see exact records for manual inspection (uncomment to run separately)
/*
SELECT 
  geolocation_zip_code_prefix,
  geolocation_city,
  geolocation_state,
  geolocation_lat,
  geolocation_lng,
  invalid_reason,
  CASE WHEN had_duplicates = true THEN 'ESTIMATED' ELSE 'ORIGINAL' END as data_source
FROM invalid_state_records
ORDER BY invalid_reason, geolocation_state, geolocation_city
LIMIT 50;
*/
