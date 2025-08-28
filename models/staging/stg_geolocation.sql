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
        -- Core columns with quality checks
        geolocation_zip_code_prefix,
        case when geolocation_zip_code_prefix is null then true else false end as geolocation_zip_code_prefix_is_null,
        case when length(cast(geolocation_zip_code_prefix as string)) != 5 then true else false end as geolocation_zip_code_prefix_invalid_length,
        
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
)

select * from with_quality_flags
