-- List all unique values for geolocation fields in stg_geolocation
-- Shows distinct combinations and individual field values

-- Part 1: Unique combinations of all three fields
WITH unique_combinations AS (
  SELECT DISTINCT
    geolocation_zip_code_prefix,
    geolocation_city,
    geolocation_state,
    COUNT(*) OVER (PARTITION BY geolocation_zip_code_prefix, geolocation_city, geolocation_state) as record_count
  FROM `project-olist-470307.dbt_olist_stg.stg_geolocation`
  WHERE geolocation_zip_code_prefix IS NOT NULL
    AND geolocation_city IS NOT NULL
    AND geolocation_state IS NOT NULL
),

-- Part 2: Unique ZIP codes with their counts
unique_zip_codes AS (
  SELECT DISTINCT
    geolocation_zip_code_prefix,
    COUNT(*) as zip_occurrence_count,
    COUNT(DISTINCT geolocation_city) as unique_cities_per_zip,
    COUNT(DISTINCT geolocation_state) as unique_states_per_zip
  FROM `project-olist-470307.dbt_olist_stg.stg_geolocation`
  WHERE geolocation_zip_code_prefix IS NOT NULL
  GROUP BY geolocation_zip_code_prefix
),

-- Part 3: Unique cities with their counts
unique_cities AS (
  SELECT DISTINCT
    geolocation_city,
    COUNT(*) as city_occurrence_count,
    COUNT(DISTINCT geolocation_zip_code_prefix) as unique_zips_per_city,
    COUNT(DISTINCT geolocation_state) as unique_states_per_city
  FROM `project-olist-470307.dbt_olist_stg.stg_geolocation`
  WHERE geolocation_city IS NOT NULL
  GROUP BY geolocation_city
),

-- Part 4: Unique states with their counts
unique_states AS (
  SELECT DISTINCT
    geolocation_state,
    COUNT(*) as state_occurrence_count,
    COUNT(DISTINCT geolocation_zip_code_prefix) as unique_zips_per_state,
    COUNT(DISTINCT geolocation_city) as unique_cities_per_state
  FROM `project-olist-470307.dbt_olist_stg.stg_geolocation`
  WHERE geolocation_state IS NOT NULL
  GROUP BY geolocation_state
)

-- Results: Summary statistics
SELECT 
  'SUMMARY' as section,
  'Total Unique ZIP Codes' as field_name,
  CAST(COUNT(*) AS STRING) as unique_count,
  'Number of distinct ZIP codes in table' as description
FROM unique_zip_codes

UNION ALL

SELECT 
  'SUMMARY' as section,
  'Total Unique Cities' as field_name,
  CAST(COUNT(*) AS STRING) as unique_count,
  'Number of distinct cities in table' as description
FROM unique_cities

UNION ALL

SELECT 
  'SUMMARY' as section,
  'Total Unique States' as field_name,
  CAST(COUNT(*) AS STRING) as unique_count,
  'Number of distinct states in table' as description
FROM unique_states

UNION ALL

SELECT 
  'SUMMARY' as section,
  'Total Unique Combinations' as field_name,
  CAST(COUNT(*) AS STRING) as unique_count,
  'Number of distinct ZIP+City+State combinations' as description
FROM unique_combinations

UNION ALL

-- Individual unique states listing
SELECT 
  'UNIQUE_STATES_LIST' as section,
  geolocation_state as field_name,
  CAST(state_occurrence_count AS STRING) as unique_count,
  CONCAT('Records: ', CAST(state_occurrence_count AS STRING), 
         ', Cities: ', CAST(unique_cities_per_state AS STRING), 
         ', ZIPs: ', CAST(unique_zips_per_state AS STRING)) as description
FROM unique_states

ORDER BY section, 
         CASE WHEN section = 'UNIQUE_STATES_LIST' THEN CAST(unique_count AS INT64) ELSE 0 END DESC,
         field_name;

-- Separate queries for detailed listings (uncomment to run individually)

-- Query 1: All unique ZIP codes with statistics
/*
SELECT 
  'ZIP_CODES' as list_type,
  geolocation_zip_code_prefix as value,
  zip_occurrence_count as total_records,
  unique_cities_per_zip as cities_count,
  unique_states_per_zip as states_count
FROM unique_zip_codes
ORDER BY zip_occurrence_count DESC, geolocation_zip_code_prefix
LIMIT 100;
*/

-- Query 2: All unique cities with statistics
/*
SELECT 
  'CITIES' as list_type,
  geolocation_city as value,
  city_occurrence_count as total_records,
  unique_zips_per_city as zips_count,
  unique_states_per_city as states_count
FROM unique_cities
ORDER BY city_occurrence_count DESC, geolocation_city
LIMIT 100;
*/

-- Query 3: All unique states with statistics
/*
SELECT 
  'STATES' as list_type,
  geolocation_state as value,
  state_occurrence_count as total_records,
  unique_zips_per_state as zips_count,
  unique_cities_per_state as cities_count
FROM unique_states
ORDER BY state_occurrence_count DESC, geolocation_state;
*/

-- Query 4: All unique combinations (ZIP + City + State)
/*
SELECT 
  'COMBINATIONS' as list_type,
  CONCAT(geolocation_zip_code_prefix, ' | ', geolocation_city, ' | ', geolocation_state) as combination,
  geolocation_zip_code_prefix,
  geolocation_city,
  geolocation_state,
  record_count
FROM unique_combinations
ORDER BY record_count DESC, geolocation_zip_code_prefix, geolocation_city
LIMIT 200;
*/

-- Query 5: Simple list of all unique values (uncomment for basic lists)
/*
-- All unique ZIP codes
SELECT DISTINCT geolocation_zip_code_prefix 
FROM `project-olist-470307.dbt_olist_stg.stg_geolocation` 
WHERE geolocation_zip_code_prefix IS NOT NULL 
ORDER BY geolocation_zip_code_prefix;

-- All unique cities  
SELECT DISTINCT geolocation_city 
FROM `project-olist-470307.dbt_olist_stg.stg_geolocation` 
WHERE geolocation_city IS NOT NULL 
ORDER BY geolocation_city;

-- All unique states
SELECT DISTINCT geolocation_state 
FROM `project-olist-470307.dbt_olist_stg.stg_geolocation` 
WHERE geolocation_state IS NOT NULL 
ORDER BY geolocation_state;
*/
