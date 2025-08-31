{{
  config(
    materialized='table',
    cluster_by=['product_category_name_english']
  )
}}

with product_base as (
    select 
        p.product_id,
        p.product_category_name,
        p.product_name_length,
        p.product_description_length,
        p.product_photos_qty,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm,
        -- Join with translation table for English category names
        coalesce(t.product_category_name_english, p.product_category_name) as product_category_name_english
    from {{ ref('stg_products') }} p
    left join {{ ref('stg_product_category_name_translation') }} t
        on p.product_category_name = t.product_category_name
),

product_with_sk as (
    select 
        -- Generate surrogate key
        {{ generate_surrogate_key(['product_id']) }} as product_sk,
        
        -- Natural key and attributes
        product_id,
        product_category_name,
        product_name_length,
        product_description_length,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm,
        product_category_name_english,
        
        -- Audit timestamp
        current_datetime() as insertion_timestamp
        
    from product_base
)

select 
    product_sk,
    product_id,
    product_category_name,
    product_name_length,
    product_description_length,
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    product_category_name_english,
    insertion_timestamp
from product_with_sk