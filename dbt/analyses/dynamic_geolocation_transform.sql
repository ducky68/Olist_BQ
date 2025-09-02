-- Dynamic Data Validation and Fixing Query for stg_geolocation
-- This query VERIFIES issues and FIXES them automatically for dbt transformation
-- Use this as the main logic in your stg_geolocation.sql model

{{ config(materialized='table') }}

WITH source_data AS (
    SELECT * FROM {{ source('olist', 'geolocation') }}
),

-- Step 1: Identify and catalog all data quality issues
data_quality_assessment AS (
    SELECT 
        *,
        -- ZIP Code Issue Detection
        CASE 
            WHEN geolocation_zip_code_prefix IS NULL THEN 'ZIP_NULL'
            WHEN LENGTH(TRIM(CAST(geolocation_zip_code_prefix AS STRING))) != 5 THEN 'ZIP_WRONG_LENGTH'
            WHEN NOT REGEXP_CONTAINS(CAST(geolocation_zip_code_prefix AS STRING), r'^[0-9]+$') THEN 'ZIP_NOT_NUMERIC'
            WHEN CAST(geolocation_zip_code_prefix AS INT64) < 1000 THEN 'ZIP_TOO_LOW'
            WHEN CAST(geolocation_zip_code_prefix AS INT64) > 99999 THEN 'ZIP_TOO_HIGH'
            ELSE 'ZIP_VALID'
        END as zip_issue,
        
        -- City Issue Detection
        CASE 
            WHEN geolocation_city IS NULL THEN 'CITY_NULL'
            WHEN LENGTH(TRIM(geolocation_city)) = 0 THEN 'CITY_EMPTY'
            WHEN LENGTH(TRIM(geolocation_city)) < 2 THEN 'CITY_TOO_SHORT'
            WHEN REGEXP_CONTAINS(geolocation_city, r'[0-9]') THEN 'CITY_CONTAINS_NUMBERS'
            WHEN REGEXP_CONTAINS(geolocation_city, r'[^a-zA-Z\s\-\'àáâãäçèéêëìíîïñòóôõöùúûüýÿÀÁÂÃÄÇÈÉÊËÌÍÎÏÑÒÓÔÕÖÙÚÛÜÝŸ]') THEN 'CITY_INVALID_CHARS'
            ELSE 'CITY_VALID'
        END as city_issue,
        
        -- State Issue Detection
        CASE 
            WHEN geolocation_state IS NULL THEN 'STATE_NULL'
            WHEN LENGTH(TRIM(geolocation_state)) != 2 THEN 'STATE_WRONG_LENGTH'
            WHEN NOT REGEXP_CONTAINS(geolocation_state, r'^[A-Za-z]{2}$') THEN 'STATE_INVALID_FORMAT'
            WHEN UPPER(TRIM(geolocation_state)) NOT IN (
                'SP', 'RJ', 'MG', 'RS', 'PR', 'SC', 'BA', 'GO', 'ES', 'PE', 'CE', 'PB', 
                'PA', 'RN', 'AL', 'MT', 'MS', 'DF', 'PI', 'SE', 'RO', 'TO', 'AC', 'AM', 'AP', 'RR'
            ) THEN 'STATE_NOT_BRAZILIAN'
            ELSE 'STATE_VALID'
        END as state_issue,
        
        -- Coordinate Issue Detection
        CASE 
            WHEN geolocation_lat IS NULL THEN 'LAT_NULL'
            WHEN geolocation_lat < -35 OR geolocation_lat > 5 THEN 'LAT_OUT_OF_BOUNDS'
            ELSE 'LAT_VALID'
        END as lat_issue,
        
        CASE 
            WHEN geolocation_lng IS NULL THEN 'LNG_NULL'
            WHEN geolocation_lng < -75 OR geolocation_lng > -30 THEN 'LNG_OUT_OF_BOUNDS'
            ELSE 'LNG_VALID'
        END as lng_issue
    FROM source_data
),

-- Step 2: Create lookup tables for common fixes
state_corrections AS (
    SELECT state_input, state_corrected FROM (
        -- Common state code corrections
        SELECT 'sp' as state_input, 'SP' as state_corrected UNION ALL
        SELECT 'rj', 'RJ' UNION ALL
        SELECT 'mg', 'MG' UNION ALL
        SELECT 'rs', 'RS' UNION ALL
        SELECT 'pr', 'PR' UNION ALL
        SELECT 'sc', 'SC' UNION ALL
        SELECT 'ba', 'BA' UNION ALL
        SELECT 'go', 'GO' UNION ALL
        SELECT 'es', 'ES' UNION ALL
        SELECT 'pe', 'PE' UNION ALL
        SELECT 'ce', 'CE' UNION ALL
        SELECT 'pb', 'PB' UNION ALL
        SELECT 'pa', 'PA' UNION ALL
        SELECT 'rn', 'RN' UNION ALL
        SELECT 'al', 'AL' UNION ALL
        SELECT 'mt', 'MT' UNION ALL
        SELECT 'ms', 'MS' UNION ALL
        SELECT 'df', 'DF' UNION ALL
        SELECT 'pi', 'PI' UNION ALL
        SELECT 'se', 'SE' UNION ALL
        SELECT 'ro', 'RO' UNION ALL
        SELECT 'to', 'TO' UNION ALL
        SELECT 'ac', 'AC' UNION ALL
        SELECT 'am', 'AM' UNION ALL
        SELECT 'ap', 'AP' UNION ALL
        SELECT 'rr', 'RR' UNION ALL
        -- Common typos
        SELECT 'SO', 'SP' UNION ALL  -- São Paulo typo
        SELECT 'RG', 'RJ' UNION ALL  -- Rio de Janeiro typo
        SELECT 'BR', 'DF'            -- Brasil -> Distrito Federal
    )
),

city_corrections AS (
    SELECT city_input, city_corrected FROM (
        -- Common city name corrections
        SELECT 'sao paulo' as city_input, 'são paulo' as city_corrected UNION ALL
        SELECT 'rio de janeiro', 'rio de janeiro' UNION ALL
        SELECT 'brasilia', 'brasília' UNION ALL
        SELECT 'belo horizonte', 'belo horizonte' UNION ALL
        -- Remove numbers from city names
        SELECT REGEXP_REPLACE('cidade1', r'[0-9]', '') as city_input, 'cidade' as city_corrected
    )
),

-- Step 3: Apply automatic fixes
cleaned_data AS (
    SELECT 
        -- Fix ZIP codes
        CASE 
            WHEN zip_issue = 'ZIP_NULL' THEN NULL
            WHEN zip_issue = 'ZIP_WRONG_LENGTH' THEN 
                CASE 
                    WHEN LENGTH(TRIM(CAST(geolocation_zip_code_prefix AS STRING))) < 5 
                    THEN LPAD(TRIM(CAST(geolocation_zip_code_prefix AS STRING)), 5, '0')
                    WHEN LENGTH(TRIM(CAST(geolocation_zip_code_prefix AS STRING))) > 5 
                    THEN LEFT(TRIM(CAST(geolocation_zip_code_prefix AS STRING)), 5)
                    ELSE TRIM(CAST(geolocation_zip_code_prefix AS STRING))
                END
            WHEN zip_issue = 'ZIP_NOT_NUMERIC' THEN 
                CASE 
                    WHEN REGEXP_CONTAINS(CAST(geolocation_zip_code_prefix AS STRING), r'^[0-9]+') 
                    THEN REGEXP_EXTRACT(CAST(geolocation_zip_code_prefix AS STRING), r'^([0-9]+)')
                    ELSE NULL
                END
            WHEN zip_issue = 'ZIP_TOO_LOW' OR zip_issue = 'ZIP_TOO_HIGH' THEN NULL
            ELSE LPAD(CAST(geolocation_zip_code_prefix AS STRING), 5, '0')
        END as geolocation_zip_code_prefix_clean,
        
        -- Fix city names
        CASE 
            WHEN city_issue = 'CITY_NULL' OR city_issue = 'CITY_EMPTY' THEN NULL
            WHEN city_issue = 'CITY_TOO_SHORT' THEN NULL
            WHEN city_issue = 'CITY_CONTAINS_NUMBERS' THEN 
                TRIM(REGEXP_REPLACE(LOWER(geolocation_city), r'[0-9]', ''))
            WHEN city_issue = 'CITY_INVALID_CHARS' THEN 
                TRIM(REGEXP_REPLACE(LOWER(geolocation_city), r'[^a-zA-Z\s\-\'àáâãäçèéêëìíîïñòóôõöùúûüýÿÀÁÂÃÄÇÈÉÊËÌÍÎÏÑÒÓÔÕÖÙÚÛÜÝŸ]', ''))
            ELSE LOWER(TRIM(geolocation_city))
        END as geolocation_city_clean,
        
        -- Fix state codes
        CASE 
            WHEN state_issue = 'STATE_NULL' THEN NULL
            WHEN state_issue = 'STATE_WRONG_LENGTH' OR state_issue = 'STATE_INVALID_FORMAT' THEN 
                COALESCE(sc.state_corrected, UPPER(TRIM(geolocation_state)))
            WHEN state_issue = 'STATE_NOT_BRAZILIAN' THEN 
                COALESCE(sc.state_corrected, NULL)
            ELSE UPPER(TRIM(geolocation_state))
        END as geolocation_state_clean,
        
        -- Fix coordinates (keep within Brazil bounds or set to NULL)
        CASE 
            WHEN lat_issue = 'LAT_NULL' THEN NULL
            WHEN lat_issue = 'LAT_OUT_OF_BOUNDS' THEN NULL
            ELSE geolocation_lat
        END as geolocation_lat_clean,
        
        CASE 
            WHEN lng_issue = 'LNG_NULL' THEN NULL
            WHEN lng_issue = 'LNG_OUT_OF_BOUNDS' THEN NULL
            ELSE geolocation_lng
        END as geolocation_lng_clean,
        
        -- Track what was fixed
        CONCAT_WS('|', 
            CASE WHEN zip_issue != 'ZIP_VALID' THEN zip_issue ELSE NULL END,
            CASE WHEN city_issue != 'CITY_VALID' THEN city_issue ELSE NULL END,
            CASE WHEN state_issue != 'STATE_VALID' THEN state_issue ELSE NULL END,
            CASE WHEN lat_issue != 'LAT_VALID' THEN lat_issue ELSE NULL END,
            CASE WHEN lng_issue != 'LNG_VALID' THEN lng_issue ELSE NULL END
        ) as issues_fixed,
        
        -- Original values for audit trail
        geolocation_zip_code_prefix as original_zip,
        geolocation_city as original_city,
        geolocation_state as original_state,
        geolocation_lat as original_lat,
        geolocation_lng as original_lng
        
    FROM data_quality_assessment dqa
    LEFT JOIN state_corrections sc ON LOWER(TRIM(dqa.geolocation_state)) = sc.state_input
),

-- Step 4: Deduplication with preference for cleaned records
deduplicated AS (
    SELECT 
        *,
        COUNT(*) OVER (PARTITION BY geolocation_zip_code_prefix_clean, geolocation_city_clean, geolocation_state_clean) as duplicate_count,
        ROW_NUMBER() OVER (
            PARTITION BY geolocation_zip_code_prefix_clean, geolocation_city_clean, geolocation_state_clean
            ORDER BY 
                -- Prefer records with coordinates
                CASE WHEN geolocation_lat_clean IS NOT NULL AND geolocation_lng_clean IS NOT NULL THEN 0 ELSE 1 END,
                -- Prefer records with fewer issues
                CASE WHEN issues_fixed IS NULL OR LENGTH(issues_fixed) = 0 THEN 0 ELSE LENGTH(issues_fixed) END,
                -- Prefer non-null original values
                CASE WHEN original_zip IS NOT NULL THEN 0 ELSE 1 END
        ) as row_num 
    FROM cleaned_data
    WHERE geolocation_zip_code_prefix_clean IS NOT NULL  -- Only keep records with valid ZIP codes
),

-- Step 5: Final output with quality flags
final_output AS (
    SELECT 
        -- Cleaned fields
        geolocation_zip_code_prefix_clean as geolocation_zip_code_prefix,
        geolocation_city_clean as geolocation_city,
        geolocation_state_clean as geolocation_state,
        geolocation_lat_clean as geolocation_lat,
        geolocation_lng_clean as geolocation_lng,
        
        -- Quality and audit flags
        CASE WHEN duplicate_count > 1 THEN true ELSE false END as had_duplicates,
        CASE WHEN issues_fixed IS NOT NULL AND LENGTH(issues_fixed) > 0 THEN true ELSE false END as was_data_cleaned,
        issues_fixed as cleaning_log,
        
        -- Data quality flags for monitoring
        CASE WHEN geolocation_zip_code_prefix IS NULL THEN true ELSE false END as zip_is_null,
        CASE WHEN geolocation_city IS NULL THEN true ELSE false END as city_is_null,
        CASE WHEN geolocation_state IS NULL THEN true ELSE false END as state_is_null,
        CASE WHEN geolocation_lat IS NULL THEN true ELSE false END as lat_is_null,
        CASE WHEN geolocation_lng IS NULL THEN true ELSE false END as lng_is_null,
        
        -- Coordinate validation flags
        CASE WHEN geolocation_lat < -35 OR geolocation_lat > 5 THEN true ELSE false END as lat_out_of_range,
        CASE WHEN geolocation_lng < -75 OR geolocation_lng > -30 THEN true ELSE false END as lng_out_of_range,
        
        -- State validation flag
        CASE WHEN geolocation_state NOT IN (
            'SP', 'RJ', 'MG', 'RS', 'PR', 'SC', 'BA', 'GO', 'ES', 'PE', 'CE', 'PB', 
            'PA', 'RN', 'AL', 'MT', 'MS', 'DF', 'PI', 'SE', 'RO', 'TO', 'AC', 'AM', 'AP', 'RR'
        ) THEN true ELSE false END as state_invalid,
        
        -- Audit timestamp
        current_timestamp() as ingestion_timestamp
        
    FROM deduplicated 
    WHERE row_num = 1  -- Keep only the best record per combination
)

SELECT * FROM final_output

-- Add missing ZIP codes from customers and sellers (from your previous dynamic enhancement)
UNION ALL

SELECT 
    -- [Previous dynamic missing locations code would go here]
    -- This integrates with your existing customer/seller ZIP enhancement
    NULL as geolocation_zip_code_prefix,  -- Placeholder - replace with actual implementation
    NULL as geolocation_city,
    NULL as geolocation_state,
    NULL as geolocation_lat,
    NULL as geolocation_lng,
    false as had_duplicates,
    true as was_data_cleaned,
    'PLACEHOLDER_FOR_DYNAMIC_ADDITIONS' as cleaning_log,
    false as zip_is_null,
    false as city_is_null,
    false as state_is_null,
    false as lat_is_null,
    false as lng_is_null,
    false as lat_out_of_range,
    false as lng_out_of_range,
    false as state_invalid,
    current_timestamp() as ingestion_timestamp
    
-- Remove this UNION ALL section and integrate your existing dynamic customer/seller logic instead
