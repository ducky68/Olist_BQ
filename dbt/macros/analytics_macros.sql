-- =============================================================================
-- ANALYTICS MACROS
-- =============================================================================
-- Reusable Jinja macros for analytics calculations and business logic

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

-- Customer Lifetime Value calculation macro
{% macro calculate_clv(total_revenue, total_orders, days_active, avg_review_score) %}
  round(
    ({{ total_revenue }} / nullif({{ total_orders }}, 0)) * 
    ({{ total_orders }} / nullif({{ days_active }}, 0) * 365) * 
    case 
      when {{ avg_review_score }} >= 4 then 2.5
      when {{ avg_review_score }} >= 3 then 1.8
      when {{ avg_review_score }} >= 2 then 1.2
      else 1.0
    end, 2
  )
{% endmacro %}

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

-- Customer lifetime value calculation for customer analytics
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

-- Seller performance tiers macro
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

-- Delivery performance calculation macro
{% macro delivery_performance_metrics() %}
    -- Delivery time calculations (in days)
    case 
        when order_delivered_customer_date is not null and order_purchase_timestamp is not null
        then date_diff(
            date(order_delivered_customer_date), 
            date(order_purchase_timestamp), 
            day
        )
        else null
    end as actual_delivery_days,
    
    case 
        when order_estimated_delivery_date is not null and order_purchase_timestamp is not null
        then date_diff(
            date(order_estimated_delivery_date), 
            date(order_purchase_timestamp), 
            day
        )
        else null
    end as estimated_delivery_days,
    
    case 
        when order_delivered_carrier_date is not null and order_purchase_timestamp is not null
        then date_diff(
            date(order_delivered_carrier_date), 
            date(order_purchase_timestamp), 
            day
        )
        else null
    end as processing_days,
    
    case 
        when order_delivered_customer_date is not null and order_delivered_carrier_date is not null
        then date_diff(
            date(order_delivered_customer_date), 
            date(order_delivered_carrier_date), 
            day
        )
        else null
    end as shipping_days,
    
    -- Delivery performance vs estimate
    case 
        when order_delivered_customer_date is not null and order_estimated_delivery_date is not null
        then date_diff(
            date(order_delivered_customer_date), 
            date(order_estimated_delivery_date), 
            day
        )
        else null
    end as delivery_vs_estimate_days
{% endmacro %}

-- Delivery performance tiers
{% macro delivery_performance_classification() %}
    case 
        when actual_delivery_days <= 7 then 'excellent'
        when actual_delivery_days <= 14 then 'good'
        when actual_delivery_days <= 21 then 'average'
        when actual_delivery_days <= 30 then 'poor'
        when actual_delivery_days > 30 then 'very_poor'
        else 'unknown'
    end as delivery_speed_tier,
    
    case 
        when delivery_vs_estimate_days <= -3 then 'early_delivery'
        when delivery_vs_estimate_days <= 0 then 'on_time_delivery'
        when delivery_vs_estimate_days <= 3 then 'slightly_late'
        when delivery_vs_estimate_days <= 7 then 'late_delivery'
        when delivery_vs_estimate_days > 7 then 'very_late'
        else 'no_estimate'
    end as delivery_accuracy_tier
{% endmacro %}

-- Brazilian region classification macro
{% macro classify_brazilian_region(state_column) %}
  case 
    when {{ state_column }} in ('SP', 'RJ', 'MG', 'ES') then 'southeast'
    when {{ state_column }} in ('RS', 'SC', 'PR') then 'south'
    when {{ state_column }} in ('GO', 'MT', 'MS', 'DF') then 'center_west'
    when {{ state_column }} in ('BA', 'SE', 'AL', 'PE', 'PB', 'RN', 'CE', 'PI', 'MA') then 'northeast'
    when {{ state_column }} in ('AC', 'AM', 'AP', 'PA', 'RO', 'RR', 'TO') then 'north'
    else 'unknown'
  end
{% endmacro %}

-- Market tier classification macro
{% macro classify_market_tier(state_column) %}
  case 
    when {{ state_column }} = 'SP' then 'tier_1_sao_paulo'
    when {{ state_column }} in ('RJ', 'MG') then 'tier_1_major_southeast'
    when {{ state_column }} in ('RS', 'PR', 'SC') then 'tier_2_south'
    when {{ state_column }} in ('BA', 'GO', 'PE') then 'tier_2_major_regional'
    when {{ state_column }} in ('CE', 'PA', 'DF', 'ES') then 'tier_3_secondary'
    else 'tier_4_emerging'
  end
{% endmacro %}

-- Shipping complexity classification macro
{% macro classify_shipping_complexity(customer_state, seller_state) %}
  case 
    when {{ customer_state }} = {{ seller_state }} then 'same_state'
    when {{ customer_state }} in ('SP', 'RJ', 'MG', 'ES') 
         and {{ seller_state }} in ('SP', 'RJ', 'MG', 'ES') then 'southeast_region'
    when {{ customer_state }} in ('RS', 'SC', 'PR') 
         and {{ seller_state }} in ('RS', 'SC', 'PR') then 'south_region'
    else 'cross_region'
  end
{% endmacro %}

-- Customer segmentation RFM-style macro
{% macro segment_customer_rfm(total_orders, total_spent, days_since_last_order) %}
  case 
    when {{ total_orders }} >= 5 and {{ total_spent }} >= 500 then 'champion'
    when {{ total_orders }} >= 3 and {{ total_spent }} >= 300 then 'loyal_customer'
    when {{ total_orders }} >= 2 and {{ total_spent }} >= 150 then 'potential_loyalist'
    when {{ total_orders }} = 1 and {{ total_spent }} >= 100 then 'new_customer_high_value'
    when {{ total_orders }} = 1 and {{ total_spent }} < 100 then 'new_customer_low_value'
    when {{ days_since_last_order }} > 365 then 'hibernating'
    when {{ days_since_last_order }} > 180 then 'at_risk'
    else 'needs_attention'
  end
{% endmacro %}

-- Seller performance tier macro
{% macro classify_seller_performance(total_revenue, avg_review_score) %}
  case 
    when {{ total_revenue }} >= 10000 and {{ avg_review_score }} >= 4.5 then 'top_performer'
    when {{ total_revenue }} >= 5000 and {{ avg_review_score }} >= 4.0 then 'high_performer'
    when {{ total_revenue }} >= 2000 and {{ avg_review_score }} >= 3.5 then 'good_performer'
    when {{ total_revenue }} >= 500 and {{ avg_review_score }} >= 3.0 then 'average_performer'
    when {{ avg_review_score }} < 3.0 and {{ total_revenue }} >= 500 then 'underperformer'
    else 'new_seller'
  end
{% endmacro %}

-- Payment behavior classification macro
{% macro classify_payment_behavior(payment_type, installments) %}
  case 
    when {{ payment_type }} = 'credit_card' and {{ installments }} = 1 then 'credit_single_payment'
    when {{ payment_type }} = 'credit_card' and {{ installments }} <= 6 then 'credit_short_term'
    when {{ payment_type }} = 'credit_card' and {{ installments }} <= 12 then 'credit_medium_term'
    when {{ payment_type }} = 'credit_card' and {{ installments }} > 12 then 'credit_long_term'
    when {{ payment_type }} = 'debit_card' then 'debit_immediate'
    when {{ payment_type }} = 'boleto' then 'boleto_traditional'
    when {{ payment_type }} = 'voucher' then 'voucher_discount'
    else 'other_payment'
  end
{% endmacro %}

-- Delivery performance classification macro
{% macro classify_delivery_performance(actual_days, estimated_days) %}
  case 
    when {{ actual_days }} <= 7 then 'excellent'
    when {{ actual_days }} <= 14 then 'good'
    when {{ actual_days }} <= 21 then 'average'
    when {{ actual_days }} <= 30 then 'poor'
    when {{ actual_days }} > 30 then 'very_poor'
    else 'unknown'
  end
{% endmacro %}

-- Safe division macro to avoid divide by zero
{% macro safe_divide(numerator, denominator, default_value=0) %}
  case 
    when {{ denominator }} != 0 and {{ denominator }} is not null 
    then {{ numerator }} / {{ denominator }}
    else {{ default_value }}
  end
{% endmacro %}

-- Percentage calculation macro
{% macro calculate_percentage(numerator, denominator, decimal_places=2) %}
  round({{ safe_divide(numerator, denominator, 0) }} * 100, {{ decimal_places }})
{% endmacro %}

-- Business quarter calculation macro (Brazil fiscal year)
{% macro brazilian_business_quarter(date_column) %}
  case 
    when extract(month from {{ date_column }}) in (1, 2, 3) then 'Q1'
    when extract(month from {{ date_column }}) in (4, 5, 6) then 'Q2'  
    when extract(month from {{ date_column }}) in (7, 8, 9) then 'Q3'
    when extract(month from {{ date_column }}) in (10, 11, 12) then 'Q4'
  end
{% endmacro %}

-- Seasonal period classification macro
{% macro classify_seasonal_period(month_number) %}
  case 
    when {{ month_number }} in (11, 12) then 'peak_season'
    when {{ month_number }} in (1, 2) then 'post_holiday'
    when {{ month_number }} in (6, 7) then 'winter_season'
    else 'regular_season'
  end
{% endmacro %}

-- Data quality flag macro
{% macro create_quality_flags(price, payment, freight, installments) %}
  case when {{ price }} <= 0 then 1 else 0 end as flag_invalid_price,
  case when {{ payment }} <= 0 then 1 else 0 end as flag_invalid_payment,
  case when {{ freight }} < 0 then 1 else 0 end as flag_invalid_freight,
  case when {{ installments }} <= 0 then 1 else 0 end as flag_invalid_installments
{% endmacro %}
