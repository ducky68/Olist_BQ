{% macro add_data_quality_flags(columns_config) %}
  {%- for column_name, config in columns_config.items() -%}
    
    -- Original column preserved
    {{ column_name }} as {{ column_name }}_raw,
    
    -- Data quality flags
    {%- if config.get('not_null', false) %}
    case when {{ column_name }} is null then true else false end as {{ column_name }}_is_null,
    {%- endif %}
    
    {%- if config.get('min_length') %}
    case when length({{ column_name }}) < {{ config.min_length }} then true else false end as {{ column_name }}_too_short,
    {%- endif %}
    
    {%- if config.get('max_length') %}
    case when length({{ column_name }}) > {{ config.max_length }} then true else false end as {{ column_name }}_too_long,
    {%- endif %}
    
    {%- if config.get('min_value') %}
    case when {{ column_name }} < {{ config.min_value }} then true else false end as {{ column_name }}_below_min,
    {%- endif %}
    
    {%- if config.get('max_value') %}
    case when {{ column_name }} > {{ config.max_value }} then true else false end as {{ column_name }}_above_max,
    {%- endif %}
    
    {%- if config.get('valid_values') %}
    case when {{ column_name }} not in ({{ config.valid_values | map('string') | join(', ') }}) then true else false end as {{ column_name }}_invalid_value,
    {%- endif %}
    
    {%- if config.get('regex_pattern') %}
    case when not regexp_contains({{ column_name }}, r'{{ config.regex_pattern }}') then true else false end as {{ column_name }}_invalid_format,
    {%- endif %}
    
    {%- if not loop.last -%},{%- endif %}
  {%- endfor %}
{% endmacro %}
