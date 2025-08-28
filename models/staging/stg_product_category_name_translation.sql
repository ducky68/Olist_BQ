{{ config(materialized='table') }}

with source as (
    select * from {{ source('olist', 'product_category_name_translation') }}
),
deduplicated as (
    select 
        *,
        count(*) over (partition by product_category_name) as duplicate_count,
        row_number() over (
            partition by product_category_name
            order by 
                case when product_category_name_english is not null then 0 else 1 end,
                length(product_category_name_english) desc,
                product_category_name_english
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
        product_category_name,
        case when product_category_name is null then true else false end as product_category_name_is_null,
        case when length(trim(product_category_name)) = 0 then true else false end as product_category_name_is_empty,
        case when length(product_category_name) > 100 then true else false end as product_category_name_too_long,
        
        product_category_name_english,
        case when product_category_name_english is null then true else false end as product_category_name_english_is_null,
        case when length(trim(product_category_name_english)) = 0 then true else false end as product_category_name_english_is_empty,
        case when length(product_category_name_english) > 100 then true else false end as product_category_name_english_too_long,
        
        -- Audit fields
        had_duplicates,
        current_timestamp() as ingestion_timestamp
    from unique_records
)

select * from with_quality_flags
