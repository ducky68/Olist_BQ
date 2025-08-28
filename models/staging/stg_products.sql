{{ config(materialized='table') }}

with source as (
    select * from {{ source('olist', 'products') }}
),
deduplicated as (
    select 
        *,
        count(*) over (partition by product_id) as duplicate_count,
        row_number() over (
            partition by product_id
            order by 
                case when product_category_name is not null then 0 else 1 end,
                case when product_weight_g is not null then 0 else 1 end,
                product_weight_g desc,
                product_category_name
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
        product_id,
        case when product_id is null then true else false end as product_id_is_null,
        case when length(trim(product_id)) = 0 then true else false end as product_id_is_empty,
        
        product_category_name,
        case when product_category_name is null then true else false end as product_category_name_is_null,
        case when length(trim(product_category_name)) = 0 then true else false end as product_category_name_is_empty,
        
        -- Rename misspelled source columns to correct names
        product_name_lenght as product_name_length,
        case when product_name_lenght is null then true else false end as product_name_length_is_null,
        case when product_name_lenght < 0 then true else false end as product_name_length_is_negative,
        case when product_name_lenght = 0 then true else false end as product_name_length_is_zero,
        case when product_name_lenght > 500 then true else false end as product_name_length_suspiciously_high,
        
        -- Rename misspelled source columns to correct names
        product_description_lenght as product_description_length,
        case when product_description_lenght is null then true else false end as product_description_length_is_null,
        case when product_description_lenght < 0 then true else false end as product_description_length_is_negative,
        case when product_description_lenght > 10000 then true else false end as product_description_length_suspiciously_high,
        
        product_photos_qty,
        case when product_photos_qty is null then true else false end as product_photos_qty_is_null,
        case when product_photos_qty < 0 then true else false end as product_photos_qty_is_negative,
        case when product_photos_qty > 50 then true else false end as product_photos_qty_suspiciously_high,
        
        product_weight_g,
        case when product_weight_g is null then true else false end as product_weight_g_is_null,
        case when product_weight_g < 0 then true else false end as product_weight_g_is_negative,
        case when product_weight_g = 0 then true else false end as product_weight_g_is_zero,
        case when product_weight_g > 50000 then true else false end as product_weight_g_suspiciously_high,
        
        product_length_cm,
        case when product_length_cm is null then true else false end as product_length_cm_is_null,
        case when product_length_cm < 0 then true else false end as product_length_cm_is_negative,
        case when product_length_cm = 0 then true else false end as product_length_cm_is_zero,
        case when product_length_cm > 300 then true else false end as product_length_cm_suspiciously_high,
        
        product_height_cm,
        case when product_height_cm is null then true else false end as product_height_cm_is_null,
        case when product_height_cm < 0 then true else false end as product_height_cm_is_negative,
        case when product_height_cm = 0 then true else false end as product_height_cm_is_zero,
        case when product_height_cm > 300 then true else false end as product_height_cm_suspiciously_high,
        
        product_width_cm,
        case when product_width_cm is null then true else false end as product_width_cm_is_null,
        case when product_width_cm < 0 then true else false end as product_width_cm_is_negative,
        case when product_width_cm = 0 then true else false end as product_width_cm_is_zero,
        case when product_width_cm > 300 then true else false end as product_width_cm_suspiciously_high,
        
        -- Audit fields
        had_duplicates,
        current_timestamp() as ingestion_timestamp
    from unique_records
)

select * from with_quality_flags
