{{
  config(
    materialized='table',
    cluster_by=['seller_state']
  )
}}

with seller_base as (
    select 
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state
    from {{ ref('stg_sellers') }}
),

seller_with_sk as (
    select 
        -- Generate surrogate key
        {{ generate_surrogate_key(['seller_id']) }} as seller_sk,
        
        -- Natural key and attributes
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,
        
        -- Audit timestamp
        current_datetime() as insertion_timestamp
        
    from seller_base
)

select 
    seller_sk,
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    insertion_timestamp
from seller_with_sk