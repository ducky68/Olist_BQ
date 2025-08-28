{{ config(materialized='table') }}

with source as (
    select * from {{ source('olist', 'orders') }}
),
deduplicated as (
    select 
        *,
        count(*) over (partition by order_id) as duplicate_count,
        row_number() over (
            partition by order_id
            order by 
                case when order_purchase_timestamp is not null then 0 else 1 end,
                order_purchase_timestamp desc,
                case when order_approved_at is not null then 0 else 1 end,
                order_approved_at desc
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
staging as (
    select
        -- Primary keys
        order_id,
        customer_id,
        
        -- Business data
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        
        -- Type conversions for timestamp fields
        safe_cast(order_purchase_timestamp as timestamp) as order_purchase_timestamp_clean,
        safe_cast(order_approved_at as timestamp) as order_approved_at_clean,
        safe_cast(order_delivered_carrier_date as timestamp) as order_delivered_carrier_date_clean,
        safe_cast(order_delivered_customer_date as timestamp) as order_delivered_customer_date_clean,
        safe_cast(order_estimated_delivery_date as timestamp) as order_estimated_delivery_date_clean,
        
        -- Data quality flags
        case when order_id is null then true else false end as missing_order_id,
        case when customer_id is null then true else false end as missing_customer_id,
        case when order_status is null then true else false end as missing_order_status,
        case when order_purchase_timestamp is null then true else false end as missing_purchase_timestamp,
        case when safe_cast(order_purchase_timestamp as timestamp) is null and order_purchase_timestamp is not null then true else false end as invalid_purchase_timestamp,
        case when safe_cast(order_approved_at as timestamp) is null and order_approved_at is not null then true else false end as invalid_approved_timestamp,
        case when safe_cast(order_delivered_carrier_date as timestamp) is null and order_delivered_carrier_date is not null then true else false end as invalid_carrier_date,
        case when safe_cast(order_delivered_customer_date as timestamp) is null and order_delivered_customer_date is not null then true else false end as invalid_customer_delivery_date,
        case when safe_cast(order_estimated_delivery_date as timestamp) is null and order_estimated_delivery_date is not null then true else false end as invalid_estimated_delivery_date,
        case when order_status not in ('delivered', 'shipped', 'processing', 'canceled', 'created', 'approved', 'invoiced', 'unavailable') then true else false end as invalid_order_status,
        
        -- Business logic flags
        case when safe_cast(order_approved_at as timestamp) < safe_cast(order_purchase_timestamp as timestamp) then true else false end as approval_before_purchase,
        case when safe_cast(order_delivered_carrier_date as timestamp) < safe_cast(order_approved_at as timestamp) then true else false end as carrier_before_approval,
        case when safe_cast(order_delivered_customer_date as timestamp) < safe_cast(order_delivered_carrier_date as timestamp) then true else false end as customer_delivery_before_carrier,
        case when safe_cast(order_delivered_customer_date as timestamp) > safe_cast(order_estimated_delivery_date as timestamp) then true else false end as delivered_after_estimated,
        
        -- Audit trail
        had_duplicates,
        current_timestamp() as ingestion_timestamp
        
    from unique_records
)

select * from staging
