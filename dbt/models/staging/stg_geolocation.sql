{{ config(materialized='table') }}

with source as (
    select * from {{ source('olist', 'geolocation') }}
),
deduplicated as (
    select 
        *,
        count(*) over (partition by geolocation_zip_code_prefix) as duplicate_count,
        row_number() over (
            partition by geolocation_zip_code_prefix
            order by 
                case when geolocation_city is not null then 0 else 1 end,
                case when geolocation_state is not null then 0 else 1 end,
                case when geolocation_lat is not null then 0 else 1 end,
                case when geolocation_lng is not null then 0 else 1 end,
                geolocation_city
        ) as row_num 
    from source
),
unique_records as (
    select 
        * except(row_num),
        case when duplicate_count > 1 then true else false end as had_duplicates
    from deduplicated 
    where row_num = 1
),

with_quality_flags as (
    select
        -- Convert ZIP code to proper 5-digit STRING format with leading zeros
        LPAD(CAST(geolocation_zip_code_prefix AS STRING), 5, '0') as geolocation_zip_code_prefix,
        case when geolocation_zip_code_prefix is null then true else false end as geolocation_zip_code_prefix_is_null,
        case when geolocation_zip_code_prefix < 1 OR geolocation_zip_code_prefix > 99999 then true else false end as geolocation_zip_code_prefix_invalid_range,
        case when LENGTH(LPAD(CAST(geolocation_zip_code_prefix AS STRING), 5, '0')) != 5 then true else false end as geolocation_zip_code_prefix_invalid_length,
        
        geolocation_lat,
        case when geolocation_lat is null then true else false end as geolocation_lat_is_null,
        case when geolocation_lat < -90 or geolocation_lat > 90 then true else false end as geolocation_lat_out_of_range,
        
        geolocation_lng,
        case when geolocation_lng is null then true else false end as geolocation_lng_is_null,
        case when geolocation_lng < -180 or geolocation_lng > 180 then true else false end as geolocation_lng_out_of_range,
        
        geolocation_city,
        case when geolocation_city is null then true else false end as geolocation_city_is_null,
        case when length(trim(geolocation_city)) = 0 then true else false end as geolocation_city_is_empty,
        
        geolocation_state,
        case when geolocation_state is null then true else false end as geolocation_state_is_null,
        case when geolocation_state not in ('SP', 'RJ', 'MG', 'RS', 'PR', 'SC', 'BA', 'GO', 'ES', 'PE', 'CE', 'PB', 'PA', 'RN', 'AL', 'MT', 'MS', 'DF', 'PI', 'SE', 'RO', 'TO', 'AC', 'AM', 'AP', 'RR') then true else false end as geolocation_state_invalid_value,
        
        -- Audit fields
        had_duplicates,
        current_timestamp() as ingestion_timestamp
    from unique_records
),

-- DYNAMIC: Find missing ZIP codes from both customers AND sellers, add them with estimated coordinates
customer_zips as (
    select distinct
        LPAD(CAST(customer_zip_code_prefix AS STRING), 5, '0') as zip_code_prefix,
        LOWER(TRIM(customer_city)) as city,
        UPPER(TRIM(customer_state)) as state,
        'CUSTOMER' as source_type
    from {{ ref('stg_customers') }}
    where customer_zip_code_prefix is not null
        and LENGTH(LPAD(CAST(customer_zip_code_prefix AS STRING), 5, '0')) = 5
        and customer_city is not null
        and customer_state is not null
),

seller_zips as (
    select distinct
        LPAD(CAST(seller_zip_code_prefix AS STRING), 5, '0') as zip_code_prefix,
        LOWER(TRIM(seller_city)) as city,
        UPPER(TRIM(seller_state)) as state,
        'SELLER' as source_type
    from {{ ref('stg_sellers') }}
    where seller_zip_code_prefix is not null
        and LENGTH(LPAD(CAST(seller_zip_code_prefix AS STRING), 5, '0')) = 5
        and seller_city is not null
        and seller_state is not null
),

-- Combine all ZIP codes from customers and sellers
all_business_zips as (
    select zip_code_prefix, city, state, source_type from customer_zips
    union all
    select zip_code_prefix, city, state, source_type from seller_zips
),

-- Deduplicate and prioritize (if same ZIP exists in both customer and seller with different city/state)
consolidated_zips as (
    select 
        zip_code_prefix,
        city,
        state,
        string_agg(distinct source_type order by source_type) as sources
    from all_business_zips
    group by zip_code_prefix, city, state
),

existing_geo_zips as (
    select distinct geolocation_zip_code_prefix
    from with_quality_flags
),

missing_zips as (
    select 
        c.zip_code_prefix,
        c.city,
        c.state,
        c.sources
    from consolidated_zips c
    left join existing_geo_zips e 
        on c.zip_code_prefix = e.geolocation_zip_code_prefix
    where e.geolocation_zip_code_prefix is null
),

-- Calculate city/state averages for coordinate estimation (Option 1)
city_state_averages as (
    select 
        LOWER(TRIM(geo.geolocation_city)) as city_normalized,
        UPPER(TRIM(geo.geolocation_state)) as state_normalized,
        AVG(geo.geolocation_lat) as avg_lat,
        AVG(geo.geolocation_lng) as avg_lng,
        COUNT(*) as reference_count
    from with_quality_flags geo
    where geo.geolocation_lat is not null 
        and geo.geolocation_lng is not null
        and geo.geolocation_lat between -35 and 5  -- Brazil bounds
        and geo.geolocation_lng between -75 and -30  -- Brazil bounds
    group by 1, 2
    having COUNT(*) >= 2  -- Only use averages with at least 2 reference points
),

-- State fallback coordinates for cases where city average is not available
state_centroids as (
    select state, lat, lng from (
        select 'SP' as state, -23.5505 as lat, -46.6333 as lng union all
        select 'RJ', -22.9068, -43.1729 union all
        select 'MG', -19.9167, -43.9345 union all
        select 'RS', -30.0346, -51.2177 union all
        select 'PR', -25.4284, -49.2733 union all
        select 'SC', -27.2423, -50.2189 union all
        select 'BA', -12.9714, -38.5014 union all
        select 'GO', -16.6864, -49.2643 union all
        select 'ES', -20.3155, -40.3128 union all
        select 'PE', -8.0476, -34.8770 union all
        select 'CE', -3.7172, -38.5433 union all
        select 'PB', -7.1195, -34.8450 union all
        select 'PA', -1.4558, -48.4902 union all
        select 'RN', -5.7945, -35.2110 union all
        select 'AL', -9.6658, -35.7350 union all
        select 'MT', -15.6014, -56.0979 union all
        select 'MS', -20.4697, -54.6201 union all
        select 'DF', -15.7942, -47.8822 union all
        select 'PI', -8.7744, -42.7019 union all
        select 'SE', -10.9472, -37.0731 union all
        select 'RO', -11.2451, -62.4693 union all
        select 'TO', -10.1753, -48.2982 union all
        select 'AC', -10.0336, -67.8099 union all
        select 'AM', -3.4168, -65.8561 union all
        select 'AP', 1.4093, -51.7929 union all
        select 'RR', 1.8890, -61.2208
    )
),

-- Generate dynamic missing geolocation records with estimated coordinates (from customers AND sellers)
dynamic_missing_locations as (
    select
        m.zip_code_prefix as geolocation_zip_code_prefix,
        false as geolocation_zip_code_prefix_is_null,
        false as geolocation_zip_code_prefix_invalid_range,
        false as geolocation_zip_code_prefix_invalid_length,
        
        -- Use city average if available, otherwise state centroid
        COALESCE(csa.avg_lat, sc.lat, -15.7942) as geolocation_lat,  -- Brazil center as final fallback
        false as geolocation_lat_is_null,
        false as geolocation_lat_out_of_range,
        
        COALESCE(csa.avg_lng, sc.lng, -47.8822) as geolocation_lng,  -- Brazil center as final fallback
        false as geolocation_lng_is_null,
        false as geolocation_lng_out_of_range,
        
        m.city as geolocation_city,
        false as geolocation_city_is_null,
        false as geolocation_city_is_empty,
        
        m.state as geolocation_state,
        false as geolocation_state_is_null,
        case when m.state not in ('SP', 'RJ', 'MG', 'RS', 'PR', 'SC', 'BA', 'GO', 'ES', 'PE', 'CE', 'PB', 'PA', 'RN', 'AL', 'MT', 'MS', 'DF', 'PI', 'SE', 'RO', 'TO', 'AC', 'AM', 'AP', 'RR') then true else false end as geolocation_state_invalid_value,
        
        true as had_duplicates,  -- Mark as synthetic/estimated (also tracks source with m.sources)
        current_timestamp() as ingestion_timestamp
    from missing_zips m
    left join city_state_averages csa 
        on csa.city_normalized = m.city 
        and csa.state_normalized = m.state
    left join state_centroids sc 
        on sc.state = m.state
),

combined_data as (
    select * from with_quality_flags
    union all
    select * from dynamic_missing_locations
),

-- APPENDED: Advanced Data Validation and Automatic Fixing Logic
data_quality_assessment as (
    select 
        *,
        -- Additional ZIP Code Validation
        case 
            when geolocation_zip_code_prefix is null then 'ZIP_NULL'
            when not regexp_contains(geolocation_zip_code_prefix, r'^[0-9]{5}$') then 'ZIP_INVALID_FORMAT'
            when cast(geolocation_zip_code_prefix as int64) < 1000 then 'ZIP_TOO_LOW'
            when cast(geolocation_zip_code_prefix as int64) > 99999 then 'ZIP_TOO_HIGH'
            else 'ZIP_VALID'
        end as zip_validation_status,
        
        -- Advanced City Validation
        case 
            when geolocation_city is null then 'CITY_NULL'
            when length(trim(geolocation_city)) = 0 then 'CITY_EMPTY'
            when length(trim(geolocation_city)) < 2 then 'CITY_TOO_SHORT'
            when regexp_contains(geolocation_city, r'[0-9]') then 'CITY_CONTAINS_NUMBERS'
            when regexp_contains(geolocation_city, r'[^a-zA-Z\s\-\'àáâãäçèéêëìíîïñòóôõöùúûüýÿÀÁÂÃÄÇÈÉÊËÌÍÎÏÑÒÓÔÕÖÙÚÛÜÝŸ]') then 'CITY_INVALID_CHARS'
            else 'CITY_VALID'
        end as city_validation_status,
        
        -- Advanced State Validation
        case 
            when geolocation_state is null then 'STATE_NULL'
            when length(trim(geolocation_state)) != 2 then 'STATE_WRONG_LENGTH'
            when not regexp_contains(geolocation_state, r'^[A-Za-z]{2}$') then 'STATE_INVALID_FORMAT'
            when upper(trim(geolocation_state)) not in (
                'SP', 'RJ', 'MG', 'RS', 'PR', 'SC', 'BA', 'GO', 'ES', 'PE', 'CE', 'PB', 
                'PA', 'RN', 'AL', 'MT', 'MS', 'DF', 'PI', 'SE', 'RO', 'TO', 'AC', 'AM', 'AP', 'RR'
            ) then 'STATE_NOT_BRAZILIAN'
            else 'STATE_VALID'
        end as state_validation_status,
        
        -- Enhanced Coordinate Validation (Brazil-specific bounds)
        case 
            when geolocation_lat is null then 'LAT_NULL'
            when geolocation_lat < -35 or geolocation_lat > 5 then 'LAT_OUT_OF_BRAZIL_BOUNDS'
            else 'LAT_VALID'
        end as lat_validation_status,
        
        case 
            when geolocation_lng is null then 'LNG_NULL'
            when geolocation_lng < -75 or geolocation_lng > -30 then 'LNG_OUT_OF_BRAZIL_BOUNDS'
            else 'LNG_VALID'
        end as lng_validation_status
    from combined_data
),

-- State correction lookup table
state_corrections as (
    select state_input, state_corrected from (
        select 'sp' as state_input, 'SP' as state_corrected union all
        select 'rj', 'RJ' union all
        select 'mg', 'MG' union all
        select 'rs', 'RS' union all
        select 'pr', 'PR' union all
        select 'sc', 'SC' union all
        select 'ba', 'BA' union all
        select 'go', 'GO' union all
        select 'es', 'ES' union all
        select 'pe', 'PE' union all
        select 'ce', 'CE' union all
        select 'pb', 'PB' union all
        select 'pa', 'PA' union all
        select 'rn', 'RN' union all
        select 'al', 'AL' union all
        select 'mt', 'MT' union all
        select 'ms', 'MS' union all
        select 'df', 'DF' union all
        select 'pi', 'PI' union all
        select 'se', 'SE' union all
        select 'ro', 'RO' union all
        select 'to', 'TO' union all
        select 'ac', 'AC' union all
        select 'am', 'AM' union all
        select 'ap', 'AP' union all
        select 'rr', 'RR' union all
        -- Common typos
        select 'SO', 'SP' union all
        select 'RG', 'RJ' union all
        select 'BR', 'DF'
    )
),

-- Apply automatic data fixes
enhanced_cleaned_data as (
    select 
        -- Enhanced ZIP code cleaning
        case 
            when zip_validation_status = 'ZIP_INVALID_FORMAT' and regexp_contains(geolocation_zip_code_prefix, r'^[0-9]+') then 
                lpad(regexp_extract(geolocation_zip_code_prefix, r'^([0-9]+)'), 5, '0')
            when zip_validation_status in ('ZIP_TOO_LOW', 'ZIP_TOO_HIGH', 'ZIP_NULL') then null
            else geolocation_zip_code_prefix
        end as geolocation_zip_code_prefix_enhanced,
        
        -- Enhanced city cleaning
        case 
            when city_validation_status in ('CITY_NULL', 'CITY_EMPTY', 'CITY_TOO_SHORT') then null
            when city_validation_status = 'CITY_CONTAINS_NUMBERS' then 
                trim(regexp_replace(lower(geolocation_city), r'[0-9]', ''))
            when city_validation_status = 'CITY_INVALID_CHARS' then 
                trim(regexp_replace(lower(geolocation_city), r'[^a-zA-Z\s\-\'àáâãäçèéêëìíîïñòóôõöùúûüýÿÀÁÂÃÄÇÈÉÊËÌÍÎÏÑÒÓÔÕÖÙÚÛÜÝŸ]', ''))
            else lower(trim(geolocation_city))
        end as geolocation_city_enhanced,
        
        -- Enhanced state cleaning
        case 
            when state_validation_status = 'STATE_NULL' then null
            when state_validation_status in ('STATE_WRONG_LENGTH', 'STATE_INVALID_FORMAT', 'STATE_NOT_BRAZILIAN') then 
                coalesce(sc.state_corrected, upper(trim(geolocation_state)))
            else upper(trim(geolocation_state))
        end as geolocation_state_enhanced,
        
        -- Enhanced coordinate cleaning (set out-of-bounds to NULL)
        case 
            when lat_validation_status in ('LAT_NULL', 'LAT_OUT_OF_BRAZIL_BOUNDS') then null
            else geolocation_lat
        end as geolocation_lat_enhanced,
        
        case 
            when lng_validation_status in ('LNG_NULL', 'LNG_OUT_OF_BRAZIL_BOUNDS') then null
            else geolocation_lng
        end as geolocation_lng_enhanced,
        
        -- Enhanced audit trail
        array_to_string(
            array(
                select x from unnest([
                    case when zip_validation_status != 'ZIP_VALID' then zip_validation_status else null end,
                    case when city_validation_status != 'CITY_VALID' then city_validation_status else null end,
                    case when state_validation_status != 'STATE_VALID' then state_validation_status else null end,
                    case when lat_validation_status != 'LAT_VALID' then lat_validation_status else null end,
                    case when lng_validation_status != 'LNG_VALID' then lng_validation_status else null end
                ]) as x where x is not null
            ), '|'
        ) as data_issues_detected,
        
        -- Keep original validation flags and other fields
        *
        
    from data_quality_assessment dqa
    left join state_corrections sc on lower(trim(dqa.geolocation_state)) = sc.state_input
),

-- Final enhanced output with comprehensive quality flags
final_enhanced_output as (
    select 
        -- Use enhanced cleaned fields
        geolocation_zip_code_prefix_enhanced as geolocation_zip_code_prefix,
        geolocation_city_enhanced as geolocation_city,
        geolocation_state_enhanced as geolocation_state,
        geolocation_lat_enhanced as geolocation_lat,
        geolocation_lng_enhanced as geolocation_lng,
        
        -- Enhanced quality flags
        case when geolocation_zip_code_prefix_enhanced is null then true else false end as geolocation_zip_code_prefix_is_null_enhanced,
        case when geolocation_city_enhanced is null then true else false end as geolocation_city_is_null_enhanced,
        case when geolocation_state_enhanced is null then true else false end as geolocation_state_is_null_enhanced,
        case when geolocation_lat_enhanced is null then true else false end as geolocation_lat_is_null_enhanced,
        case when geolocation_lng_enhanced is null then true else false end as geolocation_lng_is_null_enhanced,
        
        -- Enhanced coordinate validation (Brazil-specific)
        case when geolocation_lat_enhanced < -35 or geolocation_lat_enhanced > 5 then true else false end as geolocation_lat_out_of_brazil_bounds,
        case when geolocation_lng_enhanced < -75 or geolocation_lng_enhanced > -30 then true else false end as geolocation_lng_out_of_brazil_bounds,
        
        -- Enhanced state validation
        case when geolocation_state_enhanced not in (
            'SP', 'RJ', 'MG', 'RS', 'PR', 'SC', 'BA', 'GO', 'ES', 'PE', 'CE', 'PB', 
            'PA', 'RN', 'AL', 'MT', 'MS', 'DF', 'PI', 'SE', 'RO', 'TO', 'AC', 'AM', 'AP', 'RR'
        ) then true else false end as geolocation_state_invalid_value_enhanced,
        
        -- Data cleaning audit flags
        case when data_issues_detected is not null and length(data_issues_detected) > 0 then true else false end as data_was_cleaned,
        data_issues_detected as cleaning_log,
        
        -- Keep existing audit fields
        had_duplicates,
        ingestion_timestamp,
        
        -- Keep original quality flags for comparison
        geolocation_zip_code_prefix_is_null,
        geolocation_city_is_null,
        geolocation_state_is_null,
        geolocation_lat_is_null,
        geolocation_lng_is_null,
        geolocation_lat_out_of_range,
        geolocation_lng_out_of_range,
        geolocation_state_invalid_value
        
    from enhanced_cleaned_data
    where geolocation_zip_code_prefix_enhanced is not null  -- Only keep records with valid ZIP codes
)

select * from final_enhanced_output
