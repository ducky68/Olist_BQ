{{ config(materialized='table') }}

with source as (
    select * from {{ source('olist', 'customers') }}
),
deduplicated as (
    select 
        *,
        count(*) over (partition by customer_id) as duplicate_count,
        row_number() over (
            partition by customer_id 
            order by 
                case when customer_unique_id is not null then 0 else 1 end,
                customer_unique_id
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
        customer_id,
        case when customer_id is null then true else false end as customer_id_is_null,
        
        customer_unique_id,
        case when customer_unique_id is null then true else false end as customer_unique_id_is_null,
        
        -- Convert ZIP code to proper 5-digit STRING format with leading zeros
        LPAD(CAST(customer_zip_code_prefix AS STRING), 5, '0') as customer_zip_code_prefix,
        case when customer_zip_code_prefix is null then true else false end as customer_zip_code_prefix_is_null,
        case when customer_zip_code_prefix < 1 OR customer_zip_code_prefix > 99999 then true else false end as customer_zip_code_prefix_invalid_range,
        case when LENGTH(LPAD(CAST(customer_zip_code_prefix AS STRING), 5, '0')) != 5 then true else false end as customer_zip_code_prefix_invalid_length,
        
        customer_city,
        case when customer_city is null then true else false end as customer_city_is_null,
        case when length(trim(customer_city)) = 0 then true else false end as customer_city_is_empty,
        
        customer_state,
        case when customer_state is null then true else false end as customer_state_is_null,
        case when customer_state not in ('SP', 'RJ', 'MG', 'RS', 'PR', 'SC', 'BA', 'GO', 'ES', 'PE', 'CE', 'PB', 'PA', 'RN', 'AL', 'MT', 'MS', 'DF', 'PI', 'SE', 'RO', 'TO', 'AC', 'AM', 'AP', 'RR') then true else false end as customer_state_invalid_value,
        
        -- Audit fields
        had_duplicates,
        current_timestamp() as ingestion_timestamp
    from unique_records
)

select * from with_quality_flags