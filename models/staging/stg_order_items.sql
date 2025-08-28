{{ config(materialized='table') }}

with source as (
    select * from {{ source('olist', 'order_items') }}
),
deduplicated as (
    select 
        *,
        count(*) over (partition by order_id, order_item_id) as duplicate_count,
        row_number() over (
            partition by order_id, order_item_id
            order by 
                case when shipping_limit_date is not null then 0 else 1 end,
                shipping_limit_date desc,
                product_id
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
        order_id,
        case when order_id is null then true else false end as order_id_is_null,
        case when length(trim(order_id)) = 0 then true else false end as order_id_is_empty,
        
        order_item_id,
        case when order_item_id is null then true else false end as order_item_id_is_null,
        case when order_item_id < 1 then true else false end as order_item_id_invalid,
        
        product_id,
        case when product_id is null then true else false end as product_id_is_null,
        case when length(trim(product_id)) = 0 then true else false end as product_id_is_empty,
        
        seller_id,
        case when seller_id is null then true else false end as seller_id_is_null,
        case when length(trim(seller_id)) = 0 then true else false end as seller_id_is_empty,
        
        shipping_limit_date,
        case when shipping_limit_date is null then true else false end as shipping_limit_date_is_null,
        case when shipping_limit_date < timestamp('2016-01-01') then true else false end as shipping_limit_date_too_old,
        case when shipping_limit_date > current_timestamp() then true else false end as shipping_limit_date_is_future,
        
        price,
        case when price is null then true else false end as price_is_null,
        case when price < 0 then true else false end as price_is_negative,
        case when price = 0 then true else false end as price_is_zero,
        case when price > 10000 then true else false end as price_suspiciously_high,
        
        freight_value,
        case when freight_value is null then true else false end as freight_value_is_null,
        case when freight_value < 0 then true else false end as freight_value_is_negative,
        case when freight_value > 1000 then true else false end as freight_value_suspiciously_high,
        
        -- Audit fields
        had_duplicates,
        current_timestamp() as ingestion_timestamp
    from unique_records
)

select * from with_quality_flags
