{{
  config(
    materialized='table',
    cluster_by=['payment_type']
  )
}}

with payment_base as (
    select 
        order_id,
        payment_sequential,
        payment_type
    from {{ ref('stg_order_payments') }}
),

payment_with_sk as (
    select 
        -- Generate surrogate key
        {{ generate_surrogate_key(['order_id', 'payment_sequential']) }} as payment_sk,
        
        -- Natural key and attributes
        order_id,
        payment_sequential,
        payment_type,
        
        -- Audit timestamp
        current_datetime() as insertion_timestamp
        
    from payment_base
)

select 
    payment_sk,
    order_id,
    payment_sequential,
    payment_type,
    insertion_timestamp
from payment_with_sk