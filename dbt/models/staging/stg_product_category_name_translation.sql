{{ config(materialized='table') }}

with source as (
    select * from {{ source('olist', 'product_category_name_translation') }}
),

-- Add missing translations not present in source
missing_translations as (
    select 
        product_category_name,
        product_category_name_english,
        0 as duplicate_count,
        false as had_duplicates,
        true as is_manually_added
    from (
        select 'portateis_cozinha_e_preparadores_de_alimentos' as product_category_name, 'portable_kitchen_and_food_preparators' as product_category_name_english
        union all
        select 'pc_gamer' as product_category_name, 'pc_gamer' as product_category_name_english
    )
    where product_category_name not in (
        select distinct product_category_name 
        from {{ source('olist', 'product_category_name_translation') }}
        where product_category_name is not null
    )
),

-- Combine source data with missing translations
combined_data as (
    select 
        product_category_name,
        product_category_name_english,
        false as is_manually_added
    from source
    
    union all
    
    select 
        product_category_name,
        product_category_name_english,
        is_manually_added
    from missing_translations
),
deduplicated as (
    select 
        *,
        count(*) over (partition by product_category_name) as duplicate_count,
        row_number() over (
            partition by product_category_name
            order by 
                case when product_category_name_english is not null then 0 else 1 end,
                case when is_manually_added then 0 else 1 end,  -- Prioritize manually added translations
                length(product_category_name_english) desc,
                product_category_name_english
        ) as row_num 
    from combined_data
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
        is_manually_added,
        had_duplicates,
        current_timestamp() as ingestion_timestamp
    from unique_records
)

select * from with_quality_flags
