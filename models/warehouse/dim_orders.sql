{{
  config(
    materialized='table',
    cluster_by=['order_status']
  )
}}

with order_base as (
    select 
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date
    from {{ ref('stg_orders') }}
),

order_with_sk as (
    select 
        -- Generate surrogate key
        {{ generate_surrogate_key(['order_id']) }} as order_sk,
        
        -- Natural key and attributes
        order_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        
        -- Audit timestamp
        current_datetime() as insertion_timestamp
        
    from order_base
)

select 
    order_sk,
    order_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    insertion_timestamp
from order_with_sk