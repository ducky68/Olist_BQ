-- =============================================================================
-- PAYMENT BEHAVIOR ANALYTICS OBT
-- =============================================================================
-- Business Purpose: Payment behavior analysis and installment insights
-- Grain: One row per payment transaction (order-payment level)
-- Update Frequency: Daily
-- =============================================================================

{{
  config(
    materialized='table',
    cluster_by=['payment_type', 'installment_category', 'customer_state'],
    description='Payment behavior analytics OBT for installment analysis and payment insights'
  )
}}

-- Payment risk assessment macro
{% macro payment_risk_indicators() %}
    case 
        when payment_installments >= 12 then 'high_risk'
        when payment_installments >= 6 then 'medium_risk'
        when payment_installments >= 3 then 'low_risk'
        else 'minimal_risk'
    end as payment_risk_level,
    
    -- Credit behavior classification
    case 
        when payment_type = 'credit_card' and payment_installments = 1 then 'credit_single_payment'
        when payment_type = 'credit_card' and payment_installments <= 6 then 'credit_short_term'
        when payment_type = 'credit_card' and payment_installments <= 12 then 'credit_medium_term'
        when payment_type = 'credit_card' and payment_installments > 12 then 'credit_long_term'
        when payment_type = 'debit_card' then 'debit_immediate'
        when payment_type = 'boleto' then 'boleto_traditional'
        when payment_type = 'voucher' then 'voucher_discount'
        else 'other_payment'
    end as credit_behavior_type
{% endmacro %}

-- Payment analytics calculations
{% macro payment_analytics_metrics() %}
    -- Installment analysis
    round(allocated_payment / nullif(payment_installments, 0), 2) as payment_per_installment,
    round((allocated_payment - item_price) / nullif(item_price, 0) * 100, 2) as payment_premium_pct,
    
    -- Payment timing value
    case 
        when payment_installments = 1 then allocated_payment
        else round(allocated_payment / power(1.02, payment_installments - 1), 2)  -- Approximate NPV with 2% monthly discount
    end as payment_present_value,
    
    -- Payment burden analysis
    case 
        when item_price <= 50 then 'low_value_purchase'
        when item_price <= 200 then 'medium_value_purchase'  
        when item_price <= 500 then 'high_value_purchase'
        else 'premium_purchase'
    end as purchase_value_category
{% endmacro %}

with payment_behavior_base as (
    select 
        -- =============================================================================
        -- IDENTIFIERS (Natural Keys for Business Use)
        -- =============================================================================
        concat(order_id, '-', order_item_id) as payment_transaction_sk,
        order_id,
        order_item_id,
        customer_id,
        product_id,
        seller_id,
        
        -- =============================================================================
        -- TEMPORAL DIMENSIONS
        -- =============================================================================
        order_date,
        order_year,
        order_quarter,
        order_month,
        year_quarter,
        year_month,
        
        -- =============================================================================
        -- CUSTOMER DIMENSIONS
        -- =============================================================================
        customer_city,
        customer_state,
        
        -- =============================================================================
        -- PRODUCT DIMENSIONS
        -- =============================================================================
        product_category_english,
        product_weight_category,
        
        -- =============================================================================
        -- PAYMENT CORE METRICS
        -- =============================================================================
        payment_type,
        payment_installments,
        item_price,
        allocated_payment,
        freight_cost,
        total_item_cost,
        
        -- =============================================================================
        -- PAYMENT BEHAVIOR ANALYSIS
        -- =============================================================================
        {{ payment_analytics_metrics() }},
        {{ payment_risk_indicators() }},
        
        -- =============================================================================
        -- INSTALLMENT CATEGORIZATION
        -- =============================================================================
        case 
            when payment_installments = 1 then 'single_payment'
            when payment_installments between 2 and 3 then 'short_installment'
            when payment_installments between 4 and 6 then 'medium_installment'
            when payment_installments between 7 and 12 then 'long_installment'
            when payment_installments > 12 then 'extended_installment'
            else 'unknown'
        end as installment_category,
        
        -- =============================================================================
        -- PAYMENT METHOD PREFERENCES
        -- =============================================================================
        case 
            when payment_type = 'credit_card' then 1 else 0 
        end as is_credit_card,
        case 
            when payment_type = 'boleto' then 1 else 0 
        end as is_boleto,
        case 
            when payment_type = 'debit_card' then 1 else 0 
        end as is_debit_card,
        case 
            when payment_type = 'voucher' then 1 else 0 
        end as is_voucher,
        
        -- =============================================================================
        -- ECONOMIC INDICATORS
        -- =============================================================================
        -- Payment affordability index
        case 
            when payment_installments = 1 then 100  -- Full affordability
            when payment_installments <= 3 then 80   -- High affordability
            when payment_installments <= 6 then 60   -- Medium affordability
            when payment_installments <= 12 then 40  -- Low affordability
            else 20  -- Very low affordability
        end as affordability_index,
        
        -- Credit utilization indicator
        case 
            when payment_type = 'credit_card' and payment_installments > 6 then 'high_credit_usage'
            when payment_type = 'credit_card' and payment_installments > 1 then 'moderate_credit_usage'
            when payment_type = 'credit_card' and payment_installments = 1 then 'low_credit_usage'
            when payment_type in ('debit_card', 'boleto') then 'no_credit_usage'
            else 'alternative_payment'
        end as credit_utilization_level,
        
        -- =============================================================================
        -- PAYMENT BEHAVIOR FLAGS
        -- =============================================================================
        case when payment_installments > 12 then 1 else 0 end as flag_high_installments,
        case when allocated_payment > item_price * 1.1 then 1 else 0 end as flag_high_payment_premium,
        case when payment_type = 'not_defined' then 1 else 0 end as flag_undefined_payment,
        case when payment_installments = 0 then 1 else 0 end as flag_invalid_installments,
        
        -- =============================================================================
        -- SATISFACTION & QUALITY
        -- =============================================================================
        review_score,
        satisfaction_level,
        
        -- Payment satisfaction correlation
        case 
            when payment_installments <= 3 and review_score >= 4 then 'satisfied_quick_payer'
            when payment_installments > 6 and review_score >= 4 then 'satisfied_installment_user'
            when payment_installments <= 3 and review_score <= 2 then 'dissatisfied_quick_payer'
            when payment_installments > 6 and review_score <= 2 then 'dissatisfied_installment_user'
            else 'neutral_payment_satisfaction'
        end as payment_satisfaction_profile

    from {{ ref('revenue_analytics_obt') }}
),

-- Customer payment behavior aggregation
customer_payment_summary as (
    select 
        customer_id,
        customer_state,
        
        -- Payment method preferences (percentages)
        round(avg(is_credit_card) * 100, 1) as credit_card_usage_pct,
        round(avg(is_boleto) * 100, 1) as boleto_usage_pct,
        round(avg(is_debit_card) * 100, 1) as debit_card_usage_pct,
        round(avg(is_voucher) * 100, 1) as voucher_usage_pct,
        
        -- Installment behavior
        avg(payment_installments) as avg_installments_used,
        max(payment_installments) as max_installments_used,
        round(avg(affordability_index), 1) as avg_affordability_index,
        
        -- Payment patterns
        count(*) as total_payment_transactions,
        round(avg(payment_per_installment), 2) as avg_installment_amount,
        round(avg(payment_premium_pct), 2) as avg_payment_premium_pct
        
    from payment_behavior_base
    group by 1, 2
),

payment_analytics_obt as (
    select 
        pb.*,
        
        -- =============================================================================
        -- CUSTOMER PAYMENT PROFILE ENRICHMENT
        -- =============================================================================
        cps.credit_card_usage_pct,
        cps.boleto_usage_pct,
        cps.debit_card_usage_pct,
        cps.voucher_usage_pct,
        cps.avg_installments_used as customer_avg_installments,
        cps.max_installments_used as customer_max_installments,
        cps.avg_affordability_index as customer_affordability_index,
        cps.total_payment_transactions as customer_total_transactions,
        
        -- =============================================================================
        -- CUSTOMER PAYMENT SEGMENTATION
        -- =============================================================================
        case 
            when cps.credit_card_usage_pct >= 80 then 'credit_card_primary'
            when cps.boleto_usage_pct >= 50 then 'boleto_preferred'
            when cps.debit_card_usage_pct >= 50 then 'debit_card_preferred'
            when cps.voucher_usage_pct >= 30 then 'voucher_user'
            else 'mixed_payment_user'
        end as customer_payment_profile,
        
        case 
            when cps.avg_installments_used >= 8 then 'high_installment_user'
            when cps.avg_installments_used >= 4 then 'medium_installment_user'
            when cps.avg_installments_used >= 2 then 'low_installment_user'
            else 'single_payment_preferred'
        end as customer_installment_behavior,
        
        -- =============================================================================
        -- AUDIT FIELDS
        -- =============================================================================
        current_datetime() as last_updated_timestamp
        
    from payment_behavior_base pb
    left join customer_payment_summary cps 
        on pb.customer_id = cps.customer_id
)

select * from payment_analytics_obt
