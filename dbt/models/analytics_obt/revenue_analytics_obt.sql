-- =============================================================================
-- REVENUE ANALYTICS OBT
-- =============================================================================
-- Business Purpose: Comprehensive revenue analysis across all dimensions
-- Grain: One row per order item (most granular for flexible aggregation)
-- Update Frequency: Daily
-- =============================================================================

{{
  config(
    materialized='table',
    partition_by={
      'field': 'order_date',
      'data_type': 'date'
    },
    cluster_by=['customer_state', 'product_category_english', 'seller_state'],
    description='Revenue analytics OBT enabling analysis by product, customer, seller, and geography'
  )
}}

-- Revenue calculation macro
{% macro calculate_revenue_metrics() %}
    -- Core revenue measures
    f.price as item_price,
    f.freight_value as freight_cost,
    f.payment_value as allocated_payment,
    (f.price + f.freight_value) as total_item_cost,
    
    -- Calculated revenue metrics
    round(f.payment_value - f.price, 2) as payment_premium,
    round((f.payment_value / nullif(f.price, 0) - 1) * 100, 2) as payment_markup_pct,
    round(f.freight_value / nullif(f.price, 0) * 100, 2) as freight_to_price_ratio_pct,
    
    -- Revenue per unit metrics
    round(f.price / nullif(f.payment_installments, 0), 2) as price_per_installment,
    case 
        when f.payment_installments > 1 then 'installment'
        else 'single_payment'
    end as payment_behavior_type
{% endmacro %}

-- Geographic metrics macro
{% macro geographic_metrics() %}
    -- Distance and logistics
    case 
        when c.customer_state = s.seller_state then 'same_state'
        when c.customer_state in ('SP', 'RJ', 'MG', 'ES') and s.seller_state in ('SP', 'RJ', 'MG', 'ES') then 'southeast_region'
        when c.customer_state in ('RS', 'SC', 'PR') and s.seller_state in ('RS', 'SC', 'PR') then 'south_region'
        else 'cross_region'
    end as shipping_complexity,
    
    -- Market concentration
    case 
        when c.customer_state = 'SP' then 'sao_paulo_market'
        when c.customer_state in ('RJ', 'MG') then 'major_southeast'
        when c.customer_state in ('RS', 'PR', 'SC') then 'south_market'
        else 'other_markets'
    end as market_segment
{% endmacro %}

with revenue_analytics_obt as (
    select 
        -- =============================================================================
        -- IDENTIFIERS & KEYS (Natural Keys for Business Use)
        -- =============================================================================
        concat(f.order_id, '-', f.order_item_id) as revenue_sk,
        f.order_id,
        f.order_item_id,
        c.customer_id,
        p.product_id,
        s.seller_id,
        
        -- =============================================================================
        -- DATE DIMENSIONS
        -- =============================================================================
        d.date_value as order_date,
        d.year as order_year,
        d.quarter as order_quarter,
        d.month as order_month,
        d.day_of_week as order_day_of_week,
        d.is_weekend as is_weekend_order,
        
        -- Date hierarchy for time intelligence
        concat(d.year, '-Q', d.quarter) as year_quarter,
        format_date('%Y-%m', d.date_value) as year_month,
        format_date('%Y-W%U', d.date_value) as year_week,
        
        -- =============================================================================
        -- CUSTOMER DIMENSIONS
        -- =============================================================================
        c.customer_city,
        c.customer_state,
        c.customer_zip_code_prefix,
        
        -- =============================================================================
        -- PRODUCT DIMENSIONS  
        -- =============================================================================
        p.product_category_name as product_category_portuguese,
        p.product_category_name_english as product_category_english,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm,
        -- Product size classification
        case 
            when p.product_weight_g <= 500 then 'light'
            when p.product_weight_g <= 2000 then 'medium'
            when p.product_weight_g <= 10000 then 'heavy'
            else 'extra_heavy'
        end as product_weight_category,
        
        -- =============================================================================
        -- SELLER DIMENSIONS
        -- =============================================================================
        s.seller_city,
        s.seller_state,
        s.seller_zip_code_prefix,
        
        -- =============================================================================
        -- PAYMENT DIMENSIONS
        -- =============================================================================
        pm.payment_type,
        pm.payment_sequential,
        f.payment_installments,
        
        -- =============================================================================
        -- ORDER STATUS & REVIEW DIMENSIONS
        -- =============================================================================
        o.order_status,
        coalesce(r.review_score, 0) as review_score,
        case 
            when r.review_score >= 4 then 'satisfied'
            when r.review_score >= 3 then 'neutral'
            when r.review_score >= 1 then 'dissatisfied'
            else 'no_review'
        end as satisfaction_level,
        
        -- =============================================================================
        -- CORE REVENUE MEASURES
        -- =============================================================================
        {{ calculate_revenue_metrics() }},
        
        -- =============================================================================
        -- GEOGRAPHIC & LOGISTICS MEASURES
        -- =============================================================================
        {{ geographic_metrics() }},
        
        -- =============================================================================
        -- BUSINESS PERFORMANCE MEASURES
        -- =============================================================================
        -- Order composition metrics
        row_number() over (partition by f.order_id order by f.order_item_id) as item_sequence_in_order,
        count(*) over (partition by f.order_id) as total_items_in_order,
        sum(f.price) over (partition by f.order_id) as total_order_value,
        
        -- Customer metrics
        row_number() over (partition by c.customer_id order by d.date_value) as customer_order_sequence,
        
        -- Seller metrics  
        count(distinct f.order_id) over (partition by s.seller_id, d.year, d.month) as seller_monthly_orders,
        
        -- Product performance
        count(*) over (partition by p.product_id, d.year, d.month) as product_monthly_sales,
        
        -- =============================================================================
        -- DATA QUALITY FLAGS
        -- =============================================================================
        case when f.price <= 0 then 1 else 0 end as flag_invalid_price,
        case when f.payment_value <= 0 then 1 else 0 end as flag_invalid_payment,
        case when f.freight_value < 0 then 1 else 0 end as flag_invalid_freight,
        case when f.payment_installments <= 0 then 1 else 0 end as flag_invalid_installments,
        
        -- =============================================================================
        -- AUDIT FIELDS
        -- =============================================================================
        current_datetime() as last_updated_timestamp
        
    from {{ ref('fact_order_items') }} f
    inner join {{ ref('dim_date') }} d on f.order_date_sk = d.date_sk
    inner join {{ ref('dim_customer') }} c on f.customer_sk = c.customer_sk
    inner join {{ ref('dim_product') }} p on f.product_sk = p.product_sk
    inner join {{ ref('dim_seller') }} s on f.seller_sk = s.seller_sk
    inner join {{ ref('dim_payment') }} pm on f.payment_sk = pm.payment_sk
    inner join {{ ref('dim_orders') }} o on f.order_sk = o.order_sk
    left join {{ ref('dim_order_reviews') }} r on f.review_sk = r.review_sk
)

select * from revenue_analytics_obt
