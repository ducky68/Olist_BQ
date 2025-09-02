-- =============================================================================
-- CUSTOMER ANALYTICS OBT  
-- =============================================================================
-- Business Purpose: Customer behavior, satisfaction, and lifecycle analysis
-- Grain: One row per customer (aggregated customer-level metrics)
-- Update Frequency: Daily
-- =============================================================================

{{
  config(
    materialized='table',
    cluster_by=['customer_state', 'customer_segment', 'satisfaction_tier'],
    description='Customer analytics OBT for behavior, satisfaction, and lifecycle analysis'
  )
}}

-- Customer segmentation macro
{% macro customer_segmentation_logic() %}
    case 
        when total_orders >= 5 and total_spent >= 500 then 'champion'
        when total_orders >= 3 and total_spent >= 300 then 'loyal_customer'
        when total_orders >= 2 and total_spent >= 150 then 'potential_loyalist'
        when total_orders = 1 and total_spent >= 100 then 'new_customer_high_value'
        when total_orders = 1 and total_spent < 100 then 'new_customer_low_value'
        when days_since_last_order > 365 then 'hibernating'
        when days_since_last_order > 180 then 'at_risk'
        else 'needs_attention'
    end as customer_segment
{% endmacro %}

-- Customer lifetime value calculation
{% macro clv_calculations() %}
    -- Basic CLV components
    round(total_spent / nullif(total_orders, 0), 2) as avg_order_value,
    round(total_spent / nullif(days_as_customer, 0) * 365, 2) as annual_spending_rate,
    round(total_orders / nullif(days_as_customer, 0) * 365, 2) as annual_order_frequency,
    
    -- Predictive CLV (simplified)
    round(
        (total_spent / nullif(total_orders, 0)) * 
        (total_orders / nullif(days_as_customer, 0) * 365) * 
        case 
            when avg_review_score >= 4 then 2.5  -- High satisfaction multiplier
            when avg_review_score >= 3 then 1.8  -- Medium satisfaction 
            when avg_review_score >= 2 then 1.2  -- Low satisfaction
            else 1.0  -- No review data
        end, 2
    ) as predicted_annual_clv
{% endmacro %}

with customer_order_history as (
    select 
        customer_sk,
        customer_id,
        customer_city,
        customer_state,
        customer_zip_code_prefix,
        
        -- Order aggregations
        count(distinct order_id) as total_orders,
        sum(item_price) as total_spent,
        sum(freight_cost) as total_freight_paid,
        sum(allocated_payment) as total_payments_made,
        avg(item_price) as avg_item_price,
        
        -- Temporal metrics
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        date_diff(current_date(), max(order_date), day) as days_since_last_order,
        date_diff(max(order_date), min(order_date), day) + 1 as days_as_customer,
        
        -- Product diversity
        count(distinct product_category_english) as categories_purchased,
        count(distinct seller_id) as sellers_used,
        
        -- Payment behavior
        avg(payment_installments) as avg_installments_used,
        count(distinct payment_type) as payment_methods_used,
        
        -- Satisfaction metrics
        avg(review_score) as avg_review_score,
        sum(case when review_score >= 4 then 1 else 0 end) as positive_reviews,
        sum(case when review_score <= 2 then 1 else 0 end) as negative_reviews,
        sum(case when review_score > 0 then 1 else 0 end) as total_reviews_given,
        
        -- Geographic behavior
        count(distinct 
            case when shipping_complexity = 'same_state' then 1 end
        ) as same_state_orders,
        count(distinct 
            case when shipping_complexity = 'cross_region' then 1 end  
        ) as cross_region_orders,
        
        -- Order timing patterns
        count(distinct case when is_weekend_order then order_date end) as weekend_orders,
        count(distinct order_quarter) as quarters_active,
        count(distinct year_month) as months_active

    from {{ ref('revenue_analytics_obt') }}
    group by 1,2,3,4,5
),

customer_analytics_obt as (
    select 
        -- =============================================================================
        -- CUSTOMER IDENTIFIERS
        -- =============================================================================
        customer_sk,
        customer_id,
        customer_city,
        customer_state,
        customer_zip_code_prefix,
        
        -- =============================================================================
        -- CUSTOMER LIFECYCLE METRICS
        -- =============================================================================
        total_orders,
        total_spent,
        total_freight_paid,
        total_payments_made,
        days_as_customer,
        days_since_last_order,
        first_order_date,
        last_order_date,
        
        -- =============================================================================
        -- CUSTOMER VALUE METRICS
        -- =============================================================================
        {{ clv_calculations() }},
        
        -- =============================================================================
        -- CUSTOMER BEHAVIOR METRICS
        -- =============================================================================
        categories_purchased,
        sellers_used,
        avg_installments_used,
        payment_methods_used,
        
        -- Shopping pattern analysis
        round(categories_purchased / nullif(total_orders, 0), 2) as category_diversity_ratio,
        round(weekend_orders / nullif(total_orders, 0) * 100, 2) as weekend_shopping_pct,
        round(same_state_orders / nullif(total_orders, 0) * 100, 2) as local_shopping_pct,
        
        -- =============================================================================
        -- CUSTOMER SATISFACTION METRICS
        -- =============================================================================
        avg_review_score,
        positive_reviews,
        negative_reviews,
        total_reviews_given,
        round(positive_reviews / nullif(total_reviews_given, 0) * 100, 2) as satisfaction_rate_pct,
        round(total_reviews_given / nullif(total_orders, 0) * 100, 2) as review_completion_rate_pct,
        
        -- Satisfaction tiers
        case 
            when avg_review_score >= 4.5 then 'highly_satisfied'
            when avg_review_score >= 4.0 then 'satisfied'
            when avg_review_score >= 3.0 then 'neutral'
            when avg_review_score >= 2.0 then 'dissatisfied'
            when avg_review_score > 0 then 'highly_dissatisfied'
            else 'no_feedback'
        end as satisfaction_tier,
        
        -- =============================================================================
        -- CUSTOMER ENGAGEMENT METRICS
        -- =============================================================================
        quarters_active,
        months_active,
        round(months_active / nullif(days_as_customer, 0) * 30, 2) as engagement_consistency,
        
        -- Purchase frequency classification
        case 
            when total_orders = 1 then 'one_time_buyer'
            when days_as_customer <= 30 then 'new_repeat_buyer'
            when total_orders / (days_as_customer / 30.0) >= 1 then 'frequent_buyer'
            when total_orders / (days_as_customer / 30.0) >= 0.5 then 'regular_buyer'
            when total_orders / (days_as_customer / 30.0) >= 0.2 then 'occasional_buyer'
            else 'rare_buyer'
        end as purchase_frequency_tier,
        
        -- =============================================================================
        -- CUSTOMER SEGMENTATION
        -- =============================================================================
        {{ customer_segmentation_logic() }},
        
        -- Risk assessment
        case 
            when days_since_last_order > 365 then 'high_churn_risk'
            when days_since_last_order > 180 then 'medium_churn_risk'
            when days_since_last_order > 90 then 'low_churn_risk'
            else 'active'
        end as churn_risk_level,
        
        -- =============================================================================
        -- GEOGRAPHIC CLASSIFICATION
        -- =============================================================================
        case 
            when customer_state in ('SP', 'RJ', 'MG', 'ES') then 'southeast'
            when customer_state in ('RS', 'SC', 'PR') then 'south'
            when customer_state in ('GO', 'MT', 'MS', 'DF') then 'center_west'
            when customer_state in ('BA', 'SE', 'AL', 'PE', 'PB', 'RN', 'CE', 'PI', 'MA') then 'northeast'
            when customer_state in ('AC', 'AM', 'AP', 'PA', 'RO', 'RR', 'TO') then 'north'
            else 'unknown'
        end as geographic_region,
        
        -- Market classification
        case 
            when customer_state = 'SP' then 'tier_1_sao_paulo'
            when customer_state in ('RJ', 'MG') then 'tier_1_major_southeast'
            when customer_state in ('RS', 'PR', 'SC') then 'tier_2_south'
            when customer_state in ('BA', 'GO', 'PE') then 'tier_2_major_regional'
            else 'tier_3_emerging'
        end as market_tier,
        
        -- =============================================================================
        -- AUDIT FIELDS
        -- =============================================================================
        current_datetime() as last_updated_timestamp
        
    from customer_order_history
)

select * from customer_analytics_obt
