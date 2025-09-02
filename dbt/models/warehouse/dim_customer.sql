{{
  config(
    materialized='table',
    cluster_by=['customer_state']
  )
}}

with customer_base as (
    select 
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    from {{ ref('stg_customers') }}
),

customer_with_sk as (
    select 
        -- Generate surrogate key
        {{ generate_surrogate_key(['customer_id']) }} as customer_sk,
        
        -- Natural key and attributes
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state,
        
        -- Audit timestamp
        current_datetime() as insertion_timestamp
        
    from customer_base
)

select 
    customer_sk,
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix,
    customer_city,
    customer_state,
    insertion_timestamp
from customer_with_sk