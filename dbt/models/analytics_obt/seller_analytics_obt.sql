-- =============================================================================
-- SELLER PERFORMANCE ANALYTICS OBT
-- =============================================================================
-- Business Purpose: Seller performance evaluation and marketplace insights
-- Grain: One row per seller (aggregated seller-level metrics)
-- Update Frequency: Daily
-- =============================================================================

{{
  config(
    materialized='table',
    cluster_by=['seller_state', 'performance_tier', 'seller_segment'],
    description='Seller performance analytics OBT for marketplace evaluation and seller insights'
  )
}}

-- Seller performance classification macro
{% macro seller_performance_tiers() %}
    case 
        when total_revenue >= 10000 and avg_review_score >= 4.5 then 'top_performer'
        when total_revenue >= 5000 and avg_review_score >= 4.0 then 'high_performer'
        when total_revenue >= 2000 and avg_review_score >= 3.5 then 'good_performer'
        when total_revenue >= 500 and avg_review_score >= 3.0 then 'average_performer'
        when avg_review_score < 3.0 and total_orders >= 10 then 'underperformer'
        else 'new_seller'
    end as performance_tier
{% endmacro %}

-- Seller business metrics macro
{% macro seller_business_metrics() %}
    -- Revenue efficiency
    round(total_revenue / nullif(total_orders, 0), 2) as revenue_per_order,
    round(total_revenue / nullif(days_active, 0), 2) as daily_revenue_rate,
    round(total_orders / nullif(days_active, 0), 2) as daily_order_rate,
    
    -- Product performance  
    round(total_items_sold / nullif(unique_products_sold, 0), 2) as avg_sales_per_product,
    round(unique_products_sold / nullif(total_orders, 0), 2) as product_diversity_per_order,
    
    -- Market reach
    round(cross_state_sales / nullif(total_orders, 0) * 100, 2) as cross_state_sales_pct,
    round(total_orders / nullif(unique_customers, 0), 2) as customer_repeat_rate
{% endmacro %}

with seller_performance_base as (
    select 
        seller_id,
        seller_city,
        seller_state,
        seller_zip_code_prefix,
        
        -- Order and sales aggregations
        count(distinct order_id) as total_orders,
        count(*) as total_items_sold,
        sum(item_price) as total_revenue,
        sum(freight_cost) as total_freight_revenue,
        sum(allocated_payment) as total_payment_received,
        avg(item_price) as avg_item_price,
        
        -- Customer metrics
        count(distinct customer_id) as unique_customers,
        count(distinct customer_state) as states_served,
        count(distinct customer_city) as cities_served,
        
        -- Product metrics
        count(distinct product_id) as unique_products_sold,
        count(distinct product_category_english) as categories_sold,
        
        -- Temporal metrics
        min(order_date) as first_sale_date,
        max(order_date) as last_sale_date,
        date_diff(max(order_date), min(order_date), day) + 1 as days_active,
        date_diff(current_date(), max(order_date), day) as days_since_last_sale,
        
        -- Quality metrics
        avg(review_score) as avg_review_score,
        sum(case when review_score >= 4 then 1 else 0 end) as positive_reviews,
        sum(case when review_score <= 2 then 1 else 0 end) as negative_reviews,
        sum(case when review_score > 0 then 1 else 0 end) as total_reviews_received,
        
        -- Payment behavior
        avg(payment_installments) as avg_installments_offered,
        count(distinct payment_type) as payment_methods_accepted,
        
        -- Geographic reach
        count(case when shipping_complexity = 'cross_region' then 1 end) as cross_state_sales,
        count(case when shipping_complexity = 'same_state' then 1 end) as local_sales,
        
        -- Seasonal patterns
        count(distinct case when is_weekend_order then order_date end) as weekend_sales,
        count(distinct order_quarter) as quarters_active,
        count(distinct year_month) as months_active

    from {{ ref('revenue_analytics_obt') }}
    group by 1,2,3,4
),

seller_analytics_obt as (
    select 
        -- =============================================================================
        -- SELLER IDENTIFIERS (Natural Keys for Business Use)
        -- =============================================================================
        seller_id as seller_sk,
        seller_id,
        seller_city,
        seller_state,
        seller_zip_code_prefix,
        
        -- =============================================================================
        -- SELLER BUSINESS METRICS
        -- =============================================================================
        total_orders,
        total_items_sold,
        total_revenue,
        total_freight_revenue,
        total_payment_received,
        days_active,
        days_since_last_sale,
        first_sale_date,
        last_sale_date,
        
        -- =============================================================================
        -- SELLER PERFORMANCE CALCULATIONS
        -- =============================================================================
        {{ seller_business_metrics() }},
        
        -- =============================================================================
        -- CUSTOMER & MARKET REACH
        -- =============================================================================
        unique_customers,
        states_served,
        cities_served,
        cross_state_sales,
        local_sales,
        
        -- Market penetration metrics
        round(unique_customers / nullif(total_orders, 0), 2) as customer_acquisition_efficiency,
        round(states_served / 27.0 * 100, 2) as national_market_penetration_pct,
        
        -- =============================================================================
        -- PRODUCT PORTFOLIO METRICS
        -- =============================================================================
        unique_products_sold,
        categories_sold,
        
        -- Portfolio analysis
        case 
            when categories_sold >= 5 then 'diversified_seller'
            when categories_sold >= 3 then 'multi_category_seller'
            when categories_sold = 2 then 'dual_category_seller'
            else 'single_category_seller'
        end as seller_category_focus,
        
        -- =============================================================================
        -- SELLER QUALITY METRICS
        -- =============================================================================
        avg_review_score,
        positive_reviews,
        negative_reviews,
        total_reviews_received,
        
        -- Quality calculations
        round(positive_reviews / nullif(total_reviews_received, 0) * 100, 2) as positive_review_rate_pct,
        round(total_reviews_received / nullif(total_orders, 0) * 100, 2) as review_coverage_rate_pct,
        
        -- Customer satisfaction tier
        case 
            when avg_review_score >= 4.5 then 'excellent_quality'
            when avg_review_score >= 4.0 then 'high_quality'
            when avg_review_score >= 3.5 then 'good_quality'
            when avg_review_score >= 3.0 then 'average_quality'
            when avg_review_score > 0 then 'poor_quality'
            else 'no_reviews'
        end as quality_tier,
        
        -- =============================================================================
        -- SELLER PERFORMANCE CLASSIFICATION
        -- =============================================================================
        {{ seller_performance_tiers() }},
        
        -- Activity level classification
        case 
            when days_since_last_sale <= 7 then 'highly_active'
            when days_since_last_sale <= 30 then 'active'
            when days_since_last_sale <= 90 then 'moderately_active'
            when days_since_last_sale <= 180 then 'low_activity'
            else 'inactive'
        end as activity_level,
        
        -- Business maturity
        case 
            when days_active >= 365 and total_orders >= 100 then 'established_business'
            when days_active >= 180 and total_orders >= 50 then 'growing_business'
            when days_active >= 90 and total_orders >= 20 then 'developing_business'
            when days_active >= 30 and total_orders >= 5 then 'startup_business'
            else 'new_business'
        end as business_maturity,
        
        -- =============================================================================
        -- SELLER SEGMENTATION
        -- =============================================================================
        case 
            when total_revenue >= 20000 and unique_customers >= 100 then 'enterprise_seller'
            when total_revenue >= 10000 and unique_customers >= 50 then 'power_seller'
            when total_revenue >= 5000 and unique_customers >= 25 then 'professional_seller'
            when total_revenue >= 1000 and unique_customers >= 10 then 'regular_seller'
            when total_revenue >= 500 and unique_customers >= 5 then 'small_seller'
            else 'micro_seller'
        end as seller_segment,
        
        -- =============================================================================
        -- PAYMENT & OPERATIONAL METRICS
        -- =============================================================================
        avg_installments_offered,
        payment_methods_accepted,
        weekend_sales,
        quarters_active,
        months_active,
        
        -- Operational efficiency
        round(weekend_sales / nullif(total_orders, 0) * 100, 2) as weekend_sales_pct,
        round(months_active / nullif(days_active, 0) * 30, 2) as operational_consistency,
        
        -- =============================================================================
        -- GEOGRAPHIC CLASSIFICATION
        -- =============================================================================
        case 
            when seller_state in ('SP', 'RJ', 'MG', 'ES') then 'southeast'
            when seller_state in ('RS', 'SC', 'PR') then 'south'
            when seller_state in ('GO', 'MT', 'MS', 'DF') then 'center_west'
            when seller_state in ('BA', 'SE', 'AL', 'PE', 'PB', 'RN', 'CE', 'PI', 'MA') then 'northeast'
            when seller_state in ('AC', 'AM', 'AP', 'PA', 'RO', 'RR', 'TO') then 'north'
            else 'unknown'
        end as seller_region,
        
        -- =============================================================================
        -- GROWTH & TREND INDICATORS
        -- =============================================================================
        -- Simple growth indicator (could be enhanced with time-series analysis)
        case 
            when days_since_last_sale <= 30 and total_orders >= 10 then 'growing'
            when days_since_last_sale <= 60 and total_orders >= 5 then 'stable'
            when days_since_last_sale <= 90 then 'declining'
            else 'at_risk'
        end as growth_trend,
        
        -- =============================================================================
        -- AUDIT FIELDS
        -- =============================================================================
        current_datetime() as last_updated_timestamp
        
    from seller_performance_base
)

select * from seller_analytics_obt
