-- Data Quality Analysis for stg_geolocation table
-- Checks unique record counts and null values for each field

WITH table_stats AS (
  SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT geolocation_zip_code_prefix) as unique_zip_codes,
    COUNT(DISTINCT CONCAT(geolocation_zip_code_prefix, '|', geolocation_city, '|', geolocation_state)) as unique_zip_city_state_combinations
  FROM `project-olist-470307.dbt_olist_stg.stg_geolocation`
),

null_analysis AS (
  SELECT
    -- Core fields
    'geolocation_zip_code_prefix' as field_name,
    SUM(CASE WHEN geolocation_zip_code_prefix IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN geolocation_zip_code_prefix IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_stg.stg_geolocation`
  
  UNION ALL
  
  SELECT
    'geolocation_lat' as field_name,
    SUM(CASE WHEN geolocation_lat IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN geolocation_lat IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_stg.stg_geolocation`
  
  UNION ALL
  
  SELECT
    'geolocation_lng' as field_name,
    SUM(CASE WHEN geolocation_lng IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN geolocation_lng IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_stg.stg_geolocation`
  
  UNION ALL
  
  SELECT
    'geolocation_city' as field_name,
    SUM(CASE WHEN geolocation_city IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN geolocation_city IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_stg.stg_geolocation`
  
  UNION ALL
  
  SELECT
    'geolocation_state' as field_name,
    SUM(CASE WHEN geolocation_state IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN geolocation_state IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_stg.stg_geolocation`
  
  UNION ALL
  
  SELECT
    'ingestion_timestamp' as field_name,
    SUM(CASE WHEN ingestion_timestamp IS NULL THEN 1 ELSE 0 END) as null_count,
    COUNT(*) as total_count,
    ROUND(SUM(CASE WHEN ingestion_timestamp IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as null_percentage
  FROM `project-olist-470307.dbt_olist_stg.stg_geolocation`
),

-- Quality flag analysis
quality_flag_summary AS (
  SELECT
    'Quality Flags Summary' as analysis_type,
    SUM(CASE WHEN had_duplicates = true THEN 1 ELSE 0 END) as records_with_duplicates_or_estimated,
    SUM(CASE WHEN geolocation_zip_code_prefix_is_null = true THEN 1 ELSE 0 END) as zip_null_flags,
    SUM(CASE WHEN geolocation_lat_is_null = true THEN 1 ELSE 0 END) as lat_null_flags,
    SUM(CASE WHEN geolocation_lng_is_null = true THEN 1 ELSE 0 END) as lng_null_flags,
    SUM(CASE WHEN geolocation_lat_out_of_range = true THEN 1 ELSE 0 END) as lat_out_of_range,
    SUM(CASE WHEN geolocation_lng_out_of_range = true THEN 1 ELSE 0 END) as lng_out_of_range,
    SUM(CASE WHEN geolocation_state_invalid_value = true THEN 1 ELSE 0 END) as invalid_states
  FROM `project-olist-470307.dbt_olist_stg.stg_geolocation`
),

-- Coordinate range analysis
coordinate_analysis AS (
  SELECT
    'Coordinate Analysis' as analysis_type,
    MIN(geolocation_lat) as min_latitude,
    MAX(geolocation_lat) as max_latitude,
    AVG(geolocation_lat) as avg_latitude,
    MIN(geolocation_lng) as min_longitude,
    MAX(geolocation_lng) as max_longitude,
    AVG(geolocation_lng) as avg_longitude
  FROM `project-olist-470307.dbt_olist_stg.stg_geolocation`
  WHERE geolocation_lat IS NOT NULL AND geolocation_lng IS NOT NULL
),

-- State distribution
state_distribution AS (
  SELECT
    geolocation_state,
    COUNT(*) as zip_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
  FROM `project-olist-470307.dbt_olist_stg.stg_geolocation`
  WHERE geolocation_state IS NOT NULL
  GROUP BY geolocation_state
  ORDER BY zip_count DESC
)

-- Main results
SELECT 
  'TABLE_OVERVIEW' as section,
  CAST(total_records AS STRING) as metric_name,
  CAST(total_records AS STRING) as metric_value,
  'Total records in table' as description
FROM table_stats

UNION ALL

SELECT 
  'TABLE_OVERVIEW' as section,
  'unique_zip_codes' as metric_name,
  CAST(unique_zip_codes AS STRING) as metric_value,
  'Unique ZIP codes' as description
FROM table_stats

UNION ALL

SELECT 
  'TABLE_OVERVIEW' as section,
  'unique_combinations' as metric_name,
  CAST(unique_zip_city_state_combinations AS STRING) as metric_value,
  'Unique ZIP+City+State combinations' as description
FROM table_stats

UNION ALL

SELECT 
  'NULL_ANALYSIS' as section,
  field_name as metric_name,
  CONCAT(CAST(null_count AS STRING), ' (', CAST(null_percentage AS STRING), '%)') as metric_value,
  'Null values count and percentage' as description
FROM null_analysis

UNION ALL

SELECT 
  'QUALITY_FLAGS' as section,
  'estimated_records' as metric_name,
  CAST(records_with_duplicates_or_estimated AS STRING) as metric_value,
  'Records marked as duplicated or estimated coordinates' as description
FROM quality_flag_summary

UNION ALL

SELECT 
  'QUALITY_FLAGS' as section,
  'coordinate_issues' as metric_name,
  CAST(lat_out_of_range + lng_out_of_range AS STRING) as metric_value,
  'Records with coordinates out of valid range' as description
FROM quality_flag_summary

UNION ALL

SELECT 
  'QUALITY_FLAGS' as section,
  'invalid_states' as metric_name,
  CAST(invalid_states AS STRING) as metric_value,
  'Records with invalid Brazilian state codes' as description
FROM quality_flag_summary

UNION ALL

SELECT 
  'COORDINATES' as section,
  'lat_range' as metric_name,
  CONCAT('Min: ', CAST(ROUND(min_latitude, 4) AS STRING), ', Max: ', CAST(ROUND(max_latitude, 4) AS STRING), ', Avg: ', CAST(ROUND(avg_latitude, 4) AS STRING)) as metric_value,
  'Latitude statistics (should be between -35 and 5 for Brazil)' as description
FROM coordinate_analysis

UNION ALL

SELECT 
  'COORDINATES' as section,
  'lng_range' as metric_name,
  CONCAT('Min: ', CAST(ROUND(min_longitude, 4) AS STRING), ', Max: ', CAST(ROUND(max_longitude, 4) AS STRING), ', Avg: ', CAST(ROUND(avg_longitude, 4) AS STRING)) as metric_value,
  'Longitude statistics (should be between -75 and -30 for Brazil)' as description
FROM coordinate_analysis

ORDER BY section, metric_name;

-- Separate query for top states (uncomment to run separately)
/*
SELECT 
  'TOP_STATES' as analysis,
  geolocation_state,
  zip_count,
  percentage
FROM state_distribution
LIMIT 10;
*/
