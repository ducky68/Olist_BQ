{{
  config(
    materialized='table',
    partition_by={
      'field': 'date_value',
      'data_type': 'date',
      'granularity': 'month'
    },
    cluster_by=['year', 'month']
  )
}}

with date_spine as (
    -- Generate comprehensive date dimension from 2016-2020 (covering Olist data period)
    select 
        date_value
    from unnest(generate_date_array('2016-01-01', '2025-12-31', interval 1 day)) as date_value
),

date_attributes as (
    select 
        date_value,
        
        -- Generate surrogate key in YYYYMMDD format
        cast(format_date('%Y%m%d', date_value) as int64) as date_sk,
        
        -- Extract date components
        extract(year from date_value) as year,
        extract(quarter from date_value) as quarter,
        extract(month from date_value) as month,
        extract(day from date_value) as day_of_month,
        extract(dayofweek from date_value) as day_of_week,
        
        -- Weekend indicator (Sunday = 1, Saturday = 7)
        case 
            when extract(dayofweek from date_value) in (1, 7) then true 
            else false 
        end as is_weekend,
        
        -- Audit timestamp
        current_datetime() as insertion_timestamp
        
    from date_spine
)

select 
    date_sk,
    date_value,
    year,
    quarter,
    month,
    day_of_month,
    day_of_week,
    is_weekend,
    insertion_timestamp
from date_attributes