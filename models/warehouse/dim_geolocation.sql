{{
  config(
    materialized='table',
    cluster_by=['geolocation_state']
  )
}}

with geolocation_base as (
    select 
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        geolocation_city,
        geolocation_state
    from {{ ref('stg_geolocation') }}
    where geolocation_zip_code_prefix is not null
),

geolocation_with_sk as (
    select 
        -- Generate surrogate key
        {{ generate_surrogate_key(['geolocation_zip_code_prefix']) }} as geolocation_sk,
        
        -- Natural key and attributes
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        geolocation_city,
        geolocation_state,
        
        -- Audit timestamp
        current_datetime() as insertion_timestamp
        
    from geolocation_base
)

select 
    geolocation_sk,
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state,
    insertion_timestamp
from geolocation_with_sk