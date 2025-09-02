{{ config(materialized='table') }}

with source as (
    select * from {{ source('olist', 'order_payments') }}
),
deduplicated as (
    select 
        *,
        count(*) over (partition by order_id, payment_sequential) as duplicate_count,
        row_number() over (
            partition by order_id, payment_sequential
            order by 
                payment_value desc,
                payment_type
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
        payment_sequential,
        
        -- Business data
        payment_type,
        payment_installments,
        payment_value,
        
        -- Data quality flags
        case when order_id is null then true else false end as missing_order_id,
        case when payment_sequential is null then true else false end as missing_payment_sequential,
        case when payment_type is null then true else false end as missing_payment_type,
        case when payment_installments is null then true else false end as missing_payment_installments,
        case when payment_value is null then true else false end as missing_payment_value,
        case when payment_value < 0 then true else false end as negative_payment_value,
        case when payment_installments < 1 then true else false end as invalid_installments,
        
        -- Audit trail
        had_duplicates,
        current_timestamp() as ingestion_timestamp
        
    from unique_records
)

select * from staging
