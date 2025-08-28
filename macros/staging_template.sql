{% macro stage_with_quality_checks(source_name, table_name, columns_config) %}

{{ config(materialized='table') }}

with source as (
    select * from {{ source(source_name, table_name) }}
),

with_quality_flags as (
    select
        -- Add data quality checks using macro
        {{ add_data_quality_flags(columns_config) }},
        
        -- Standard audit field
        current_timestamp() as ingestion_timestamp
    from source
)

select * from with_quality_flags

{% endmacro %}
