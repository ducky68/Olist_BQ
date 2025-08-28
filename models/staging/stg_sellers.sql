{{ config(materialized='table') }}

with source as (
    select * from {{ source('olist', 'sellers') }}
),
deduplicated as (
    select 
        *,
        count(*) over (partition by seller_id) as duplicate_count,
        row_number() over (
            partition by seller_id
            order by 
                case when seller_city is not null then 0 else 1 end,
                case when seller_state is not null then 0 else 1 end,
                seller_city,
                seller_state
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
        seller_id,
        case when seller_id is null then true else false end as seller_id_is_null,
        case when length(trim(seller_id)) = 0 then true else false end as seller_id_is_empty,
        
        seller_zip_code_prefix,
        case when seller_zip_code_prefix is null then true else false end as seller_zip_code_prefix_is_null,
        case when length(cast(seller_zip_code_prefix as string)) != 5 then true else false end as seller_zip_code_prefix_invalid_length,
        
        seller_city,
        case when seller_city is null then true else false end as seller_city_is_null,
        case when length(trim(seller_city)) = 0 then true else false end as seller_city_is_empty,
        
        seller_state,
        case when seller_state is null then true else false end as seller_state_is_null,
        case when seller_state not in ('SP', 'RJ', 'MG', 'RS', 'PR', 'SC', 'BA', 'GO', 'ES', 'PE', 'CE', 'PB', 'PA', 'RN', 'AL', 'MT', 'MS', 'DF', 'PI', 'SE', 'RO', 'TO', 'AC', 'AM', 'AP', 'RR') then true else false end as seller_state_invalid_value,
        
        -- Audit fields
        had_duplicates,
        current_timestamp() as ingestion_timestamp
    from unique_records
)

select * from with_quality_flags
