-- =============================================================================
-- GEOGRAPHIC MARKET ANALYTICS OBT
-- =============================================================================
-- Business Purpose: Geographic market analysis and regional performance insights
-- Grain: One row per geographic market segment (state/region level)
-- Update Frequency: Daily
-- =============================================================================

{{
  config(
    materialized='table',
    cluster_by=['geographic_region', 'market_tier', 'state_code'],
    description='Geographic market analytics OBT for regional performance and market analysis'
  )
}}

-- Regional economic indicators macro
{% macro regional_economic_metrics() %}
    -- Market concentration
    round(total_revenue / nullif(total_customers, 0), 2) as revenue_per_customer,
    round(total_orders / nullif(total_customers, 0), 2) as orders_per_customer,
    round(total_revenue / nullif(total_orders, 0), 2) as average_order_value,
    
    -- Market efficiency
    round(total_revenue / nullif(total_sellers, 0), 2) as revenue_per_seller,
    round(total_customers / nullif(total_sellers, 0), 2) as customers_per_seller,
    
    -- Economic activity indicators
    round(total_revenue / nullif(days_active, 0), 2) as daily_revenue_rate,
    round(total_orders / nullif(days_active, 0), 2) as daily_order_rate
{% endmacro %}

-- Market development classification
{% macro market_development_tier() %}
    case 
        when total_revenue >= 100000 and total_customers >= 1000 then 'tier_1_developed'
        when total_revenue >= 50000 and total_customers >= 500 then 'tier_2_growing'
        when total_revenue >= 20000 and total_customers >= 200 then 'tier_3_emerging'
        when total_revenue >= 5000 and total_customers >= 50 then 'tier_4_developing'
        else 'tier_5_nascent'
    end as market_development_tier
{% endmacro %}

with geographic_base as (
    select 
        customer_state as state_code,
        customer_city,
        
        -- Aggregate core metrics
        count(distinct order_id) as total_orders,
        count(*) as total_items_sold,
        count(distinct customer_id) as total_customers,
        count(distinct seller_id) as total_sellers,
        count(distinct product_id) as total_products_sold,
        count(distinct product_category_english) as total_categories_sold,
        
        -- Revenue metrics
        sum(item_price) as total_revenue,
        sum(freight_cost) as total_freight_revenue,
        sum(allocated_payment) as total_payment_received,
        avg(item_price) as avg_item_price,
        
        -- Temporal metrics
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        date_diff(max(order_date), min(order_date), day) + 1 as days_active,
        
        -- Quality metrics
        avg(review_score) as avg_review_score,
        sum(case when review_score >= 4 then 1 else 0 end) as positive_reviews,
        sum(case when review_score > 0 then 1 else 0 end) as total_reviews,
        
        -- Payment behavior
        avg(payment_installments) as avg_installments_used,
        count(case when payment_type = 'credit_card' then 1 end) as credit_card_transactions,
        count(case when payment_type = 'boleto' then 1 end) as boleto_transactions,
        
        -- Logistics metrics
        count(case when shipping_complexity = 'same_state' then 1 end) as local_orders,
        count(case when shipping_complexity = 'cross_region' then 1 end) as cross_region_orders,
        
        -- Seasonal patterns
        count(case when is_weekend_order then 1 end) as weekend_orders,
        count(distinct order_quarter) as quarters_active,
        count(distinct year_month) as months_active

    from {{ ref('revenue_analytics_obt') }}
    group by 1, 2
),

state_level_aggregation as (
    select 
        state_code,
        
        -- Core business metrics
        sum(total_orders) as total_orders,
        sum(total_items_sold) as total_items_sold,
        sum(total_customers) as total_customers,
        sum(total_sellers) as total_sellers,
        sum(total_products_sold) as total_products_sold,
        max(total_categories_sold) as total_categories_available,
        count(distinct customer_city) as total_cities,
        
        -- Revenue aggregation
        sum(total_revenue) as total_revenue,
        sum(total_freight_revenue) as total_freight_revenue,
        sum(total_payment_received) as total_payment_received,
        avg(avg_item_price) as avg_item_price,
        
        -- Temporal metrics
        min(first_order_date) as first_order_date,
        max(last_order_date) as last_order_date,
        max(days_active) as days_active,
        
        -- Quality metrics  
        avg(avg_review_score) as avg_review_score,
        sum(positive_reviews) as positive_reviews,
        sum(total_reviews) as total_reviews,
        
        -- Payment behavior
        avg(avg_installments_used) as avg_installments_used,
        sum(credit_card_transactions) as credit_card_transactions,
        sum(boleto_transactions) as boleto_transactions,
        
        -- Logistics metrics
        sum(local_orders) as local_orders,
        sum(cross_region_orders) as cross_region_orders,
        
        -- Activity patterns
        sum(weekend_orders) as weekend_orders,
        max(quarters_active) as quarters_active,
        max(months_active) as months_active

    from geographic_base
    group by 1
),

geographic_analytics_obt as (
    select 
        -- =============================================================================
        -- GEOGRAPHIC IDENTIFIERS
        -- =============================================================================
        state_code,
        total_cities,
        
        -- =============================================================================
        -- REGIONAL CLASSIFICATION
        -- =============================================================================
        case 
            when state_code in ('SP', 'RJ', 'MG', 'ES') then 'southeast'
            when state_code in ('RS', 'SC', 'PR') then 'south'
            when state_code in ('GO', 'MT', 'MS', 'DF') then 'center_west'
            when state_code in ('BA', 'SE', 'AL', 'PE', 'PB', 'RN', 'CE', 'PI', 'MA') then 'northeast'
            when state_code in ('AC', 'AM', 'AP', 'PA', 'RO', 'RR', 'TO') then 'north'
            else 'unknown'
        end as geographic_region,
        
        -- Market tier classification
        case 
            when state_code = 'SP' then 'tier_1_sao_paulo'
            when state_code in ('RJ', 'MG') then 'tier_1_major_southeast'
            when state_code in ('RS', 'PR', 'SC') then 'tier_2_south'
            when state_code in ('BA', 'GO', 'PE') then 'tier_2_major_regional'
            when state_code in ('CE', 'PA', 'DF', 'ES') then 'tier_3_secondary'
            else 'tier_4_emerging'
        end as market_tier,
        
        -- =============================================================================
        -- CORE BUSINESS METRICS
        -- =============================================================================
        total_orders,
        total_items_sold,
        total_customers,
        total_sellers,
        total_products_sold,
        total_categories_available,
        total_revenue,
        total_freight_revenue,
        total_payment_received,
        
        -- =============================================================================
        -- CALCULATED PERFORMANCE METRICS
        -- =============================================================================
        {{ regional_economic_metrics() }},
        
        -- =============================================================================
        -- MARKET DEVELOPMENT ANALYSIS
        -- =============================================================================
        {{ market_development_tier() }},
        
        -- Market maturity indicators
        case 
            when days_active >= 700 and total_customers >= 500 then 'mature_market'
            when days_active >= 365 and total_customers >= 200 then 'developing_market'
            when days_active >= 180 and total_customers >= 50 then 'emerging_market'
            else 'new_market'
        end as market_maturity,
        
        -- =============================================================================
        -- MARKET CONCENTRATION METRICS
        -- =============================================================================
        round(total_customers / nullif(total_cities, 0), 2) as customers_per_city,
        round(total_sellers / nullif(total_cities, 0), 2) as sellers_per_city,
        round(total_revenue / nullif(total_cities, 0), 2) as revenue_per_city,
        
        -- Market density indicators
        case 
            when total_customers / nullif(total_cities, 0) >= 50 then 'high_density'
            when total_customers / nullif(total_cities, 0) >= 20 then 'medium_density'
            when total_customers / nullif(total_cities, 0) >= 5 then 'low_density'
            else 'sparse_density'
        end as market_density,
        
        -- =============================================================================
        -- CUSTOMER BEHAVIOR ANALYSIS
        -- =============================================================================
        avg_review_score,
        round(positive_reviews / nullif(total_reviews, 0) * 100, 2) as customer_satisfaction_rate_pct,
        avg_installments_used,
        
        -- Payment preferences
        round(credit_card_transactions / nullif(total_orders, 0) * 100, 2) as credit_card_usage_pct,
        round(boleto_transactions / nullif(total_orders, 0) * 100, 2) as boleto_usage_pct,
        
        -- Payment behavior classification
        case 
            when credit_card_transactions / nullif(total_orders, 0) >= 0.7 then 'credit_card_dominant'
            when boleto_transactions / nullif(total_orders, 0) >= 0.4 then 'boleto_prevalent'
            else 'mixed_payment_region'
        end as payment_preference_profile,
        
        -- =============================================================================
        -- LOGISTICS & SHIPPING ANALYSIS
        -- =============================================================================
        local_orders,
        cross_region_orders,
        round(local_orders / nullif(total_orders, 0) * 100, 2) as local_shipping_pct,
        round(cross_region_orders / nullif(total_orders, 0) * 100, 2) as cross_region_shipping_pct,
        
        -- Logistics complexity
        case 
            when local_orders / nullif(total_orders, 0) >= 0.8 then 'locally_focused'
            when cross_region_orders / nullif(total_orders, 0) >= 0.3 then 'cross_region_active'
            else 'balanced_shipping'
        end as logistics_profile,
        
        -- =============================================================================
        -- TEMPORAL & SEASONAL PATTERNS
        -- =============================================================================
        weekend_orders,
        round(weekend_orders / nullif(total_orders, 0) * 100, 2) as weekend_activity_pct,
        quarters_active,
        months_active,
        days_active,
        first_order_date,
        last_order_date,
        
        -- Activity consistency
        round(months_active / nullif(days_active, 0) * 30, 2) as market_activity_consistency,
        
        -- =============================================================================
        -- COMPETITIVE LANDSCAPE
        -- =============================================================================
        -- Seller competition intensity
        case 
            when total_sellers / nullif(total_customers, 0) >= 0.1 then 'high_competition'
            when total_sellers / nullif(total_customers, 0) >= 0.05 then 'medium_competition'
            when total_sellers / nullif(total_customers, 0) >= 0.02 then 'low_competition'
            else 'limited_competition'
        end as seller_competition_level,
        
        -- Market opportunity index
        round(
            (total_customers / 1000.0) * 0.3 +  -- Customer base weight
            (total_revenue / 10000.0) * 0.4 +   -- Revenue weight  
            (avg_review_score / 5.0) * 0.2 +    -- Satisfaction weight
            (days_active / 365.0) * 0.1,        -- Maturity weight
            2
        ) as market_opportunity_index,
        
        -- =============================================================================
        -- AUDIT FIELDS
        -- =============================================================================
        current_datetime() as last_updated_timestamp
        
    from state_level_aggregation
)

select * from geographic_analytics_obt
