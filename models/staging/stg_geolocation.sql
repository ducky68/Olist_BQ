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

-- Add additional known geolocation records
additional_locations as (
    select
        '71551' as geolocation_zip_code_prefix,
        false as geolocation_zip_code_prefix_is_null,
        false as geolocation_zip_code_prefix_invalid_range,
        false as geolocation_zip_code_prefix_invalid_length,
        
        null as geolocation_lat,
        true as geolocation_lat_is_null,
        false as geolocation_lat_out_of_range,
        
        null as geolocation_lng,
        true as geolocation_lng_is_null,
        false as geolocation_lng_out_of_range,
        
        'brasilia' as geolocation_city,
        false as geolocation_city_is_null,
        false as geolocation_city_is_empty,
        
        'DF' as geolocation_state,
        false as geolocation_state_is_null,
        false as geolocation_state_invalid_value,
        
        false as had_duplicates,
        current_timestamp() as ingestion_timestamp
    
    union all
    
    select
        '72580' as geolocation_zip_code_prefix,
        false as geolocation_zip_code_prefix_is_null,
        false as geolocation_zip_code_prefix_invalid_range,
        false as geolocation_zip_code_prefix_invalid_length,
        
        null as geolocation_lat,
        true as geolocation_lat_is_null,
        false as geolocation_lat_out_of_range,
        
        null as geolocation_lng,
        true as geolocation_lng_is_null,
        false as geolocation_lng_out_of_range,
        
        'brasilia' as geolocation_city,
        false as geolocation_city_is_null,
        false as geolocation_city_is_empty,
        
        'DF' as geolocation_state,
        false as geolocation_state_is_null,
        false as geolocation_state_invalid_value,
        
        false as had_duplicates,
        current_timestamp() as ingestion_timestamp
    
    union all
    
    select
        '37708' as geolocation_zip_code_prefix,
        false as geolocation_zip_code_prefix_is_null,
        false as geolocation_zip_code_prefix_invalid_range,
        false as geolocation_zip_code_prefix_invalid_length,
        
        null as geolocation_lat,
        true as geolocation_lat_is_null,
        false as geolocation_lat_out_of_range,
        
        null as geolocation_lng,
        true as geolocation_lng_is_null,
        false as geolocation_lng_out_of_range,
        
        'pocos de caldas' as geolocation_city,
        false as geolocation_city_is_null,
        false as geolocation_city_is_empty,
        
        'MG' as geolocation_state,
        false as geolocation_state_is_null,
        false as geolocation_state_invalid_value,
        
        false as had_duplicates,
        current_timestamp() as ingestion_timestamp
    
    union all
    
    select
        '82040' as geolocation_zip_code_prefix,
        false as geolocation_zip_code_prefix_is_null,
        false as geolocation_zip_code_prefix_invalid_range,
        false as geolocation_zip_code_prefix_invalid_length,
        
        null as geolocation_lat,
        true as geolocation_lat_is_null,
        false as geolocation_lat_out_of_range,
        
        null as geolocation_lng,
        true as geolocation_lng_is_null,
        false as geolocation_lng_out_of_range,
        
        'curitiba' as geolocation_city,
        false as geolocation_city_is_null,
        false as geolocation_city_is_empty,
        
        'PR' as geolocation_state,
        false as geolocation_state_is_null,
        false as geolocation_state_invalid_value,
        
        false as had_duplicates,
        current_timestamp() as ingestion_timestamp
    
    union all
    
    select
        '91901' as geolocation_zip_code_prefix,
        false as geolocation_zip_code_prefix_is_null,
        false as geolocation_zip_code_prefix_invalid_range,
        false as geolocation_zip_code_prefix_invalid_length,
        
        null as geolocation_lat,
        true as geolocation_lat_is_null,
        false as geolocation_lat_out_of_range,
        
        null as geolocation_lng,
        true as geolocation_lng_is_null,
        false as geolocation_lng_out_of_range,
        
        'porto alegre' as geolocation_city,
        false as geolocation_city_is_null,
        false as geolocation_city_is_empty,
        
        'RS' as geolocation_state,
        false as geolocation_state_is_null,
        false as geolocation_state_invalid_value,
        
        false as had_duplicates,
        current_timestamp() as ingestion_timestamp
    
    union all
    
    select
        '02285' as geolocation_zip_code_prefix,
        false as geolocation_zip_code_prefix_is_null,
        false as geolocation_zip_code_prefix_invalid_range,
        false as geolocation_zip_code_prefix_invalid_length,
        
        null as geolocation_lat,
        true as geolocation_lat_is_null,
        false as geolocation_lat_out_of_range,
        
        null as geolocation_lng,
        true as geolocation_lng_is_null,
        false as geolocation_lng_out_of_range,
        
        'sao paulo' as geolocation_city,
        false as geolocation_city_is_null,
        false as geolocation_city_is_empty,
        
        'SP' as geolocation_state,
        false as geolocation_state_is_null,
        false as geolocation_state_invalid_value,
        
        false as had_duplicates,
        current_timestamp() as ingestion_timestamp
    
    union all
    
    select
        '07412' as geolocation_zip_code_prefix,
        false as geolocation_zip_code_prefix_is_null,
        false as geolocation_zip_code_prefix_invalid_range,
        false as geolocation_zip_code_prefix_invalid_length,
        
        null as geolocation_lat,
        true as geolocation_lat_is_null,
        false as geolocation_lat_out_of_range,
        
        null as geolocation_lng,
        true as geolocation_lng_is_null,
        false as geolocation_lng_out_of_range,
        
        'aruja' as geolocation_city,
        false as geolocation_city_is_null,
        false as geolocation_city_is_empty,
        
        'SP' as geolocation_state,
        false as geolocation_state_is_null,
        false as geolocation_state_invalid_value,
        
        false as had_duplicates,
        current_timestamp() as ingestion_timestamp
),

combined_data as (
    select * from with_quality_flags
    union all
    select * from additional_locations
)

select * from combined_data
