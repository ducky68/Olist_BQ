-- =============================================================================
-- ORDERS ANALYTICS OBT
-- =============================================================================
-- Business Purpose: Comprehensive order-level analysis and insights
-- Grain: One row per order (aggregated order-level metrics)
-- Update Frequency: Daily
-- =============================================================================

{{
  config(
    materialized='table',
    partition_by={
      'field': 'order_date',
      'data_type': 'date'
    },
    cluster_by=['customer_state', 'order_status'],
    description='Orders analytics OBT for comprehensive order-level analysis and insights'
  )
}}

with order_items_aggregated as (
    select 
        f.order_id,
        max(f.order_sk) as order_sk,  -- Get one order_sk per order
        max(f.customer_sk) as customer_sk,  -- Get customer_sk
        max(f.order_date_sk) as order_date_sk,  -- Get date_sk
        
        -- Order item aggregations
        count(*) as total_items,
        count(distinct f.seller_sk) as total_sellers,
        count(distinct f.product_sk) as total_products,
        count(distinct p.product_category_name_english) as total_categories,
        
        -- Financial aggregations
        sum(f.price) as total_item_value,
        sum(f.freight_value) as total_freight_value,
        sum(f.payment_value) as total_payment_value,
        avg(f.price) as avg_item_price,
        max(f.price) as max_item_price,
        min(f.price) as min_item_price,
        
        -- Payment behavior
        avg(f.payment_installments) as avg_installments,
        max(f.payment_installments) as max_installments,
        count(distinct pm.payment_type) as payment_methods_used,
        
        -- Product characteristics
        avg(p.product_weight_g) as avg_product_weight,
        sum(p.product_weight_g) as total_order_weight,
        avg(p.product_length_cm * p.product_width_cm * p.product_height_cm) as avg_product_volume,
        
        -- Geographic diversity
        count(distinct s.seller_state) as seller_states_count,
        count(distinct s.seller_city) as seller_cities_count,
        
        -- Review aggregation
        avg(f.review_score) as avg_review_score,
        count(case when f.review_score >= 4 then 1 end) as positive_reviews,
        count(case when f.review_score <= 2 then 1 end) as negative_reviews,
        count(case when f.review_score > 0 then 1 end) as total_reviews
        
    from {{ ref('fact_order_items') }} f
    inner join {{ ref('dim_product') }} p on f.product_sk = p.product_sk
    inner join {{ ref('dim_seller') }} s on f.seller_sk = s.seller_sk
    inner join {{ ref('dim_payment') }} pm on f.payment_sk = pm.payment_sk
    group by f.order_id
),

orders_analytics_obt as (
    select 
        -- =============================================================================
        -- ORDER IDENTIFIERS (Natural Keys for Business Use)
        -- =============================================================================
        oia.order_id as order_sk,
        oia.order_id,
        c.customer_id,
        c.customer_unique_id,
        
        -- =============================================================================
        -- TEMPORAL DIMENSIONS
        -- =============================================================================
        d.date_value as order_date,
        o.order_purchase_timestamp,
        o.order_approved_at,
        o.order_delivered_carrier_date,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date as estimated_delivery_date,
        
        -- Date components for analysis
        d.year as order_year,
        d.quarter as order_quarter,
        d.month as order_month,
        d.day_of_week as order_day_of_week,
        d.is_weekend as is_weekend_order,
        
        -- Time calculations
        date_diff(o.order_approved_at, o.order_purchase_timestamp, hour) as approval_time_hours,
        date_diff(o.order_delivered_carrier_date, o.order_approved_at, day) as carrier_pickup_days,
        date_diff(o.order_delivered_customer_date, o.order_delivered_carrier_date, day) as delivery_days,
        date_diff(o.order_delivered_customer_date, o.order_purchase_timestamp, day) as total_fulfillment_days,
        
        -- =============================================================================
        -- CUSTOMER DIMENSIONS
        -- =============================================================================
        c.customer_city,
        c.customer_state,
        c.customer_zip_code_prefix,
        
        -- =============================================================================
        -- ORDER STATUS & TRACKING
        -- =============================================================================
        o.order_status,
        
        -- Delivery performance classification
        case 
            when o.order_status = 'delivered' and o.order_estimated_delivery_date is not null and o.order_delivered_customer_date is not null then
                case 
                    when date_diff(o.order_delivered_customer_date, o.order_estimated_delivery_date, day) <= -2 then 'early_delivery'
                    when date_diff(o.order_delivered_customer_date, o.order_estimated_delivery_date, day) <= 1 then 'on_time_delivery'
                    when date_diff(o.order_delivered_customer_date, o.order_estimated_delivery_date, day) <= 7 then 'slightly_late'
                    when date_diff(o.order_delivered_customer_date, o.order_estimated_delivery_date, day) <= 14 then 'late_delivery'
                    else 'very_late_delivery'
                end
            when o.order_status = 'delivered' then 'delivered_no_estimate'
            when o.order_status = 'shipped' then 'in_transit'
            when o.order_status = 'canceled' then 'canceled'
            else 'processing'
        end as delivery_performance,
        
        -- Order timeline status
        case 
            when o.order_approved_at is null then 'pending_approval'
            when o.order_delivered_carrier_date is null then 'approved_not_shipped'
            when o.order_delivered_customer_date is null then 'shipped_not_delivered'
            else 'completed'
        end as fulfillment_stage,
        
        -- =============================================================================
        -- ORDER COMPOSITION METRICS
        -- =============================================================================
        oia.total_items,
        oia.total_sellers,
        oia.total_products,
        oia.total_categories,
        oia.seller_states_count,
        oia.seller_cities_count,
        
        -- Order complexity analysis
        case 
            when oia.total_items = 1 and oia.total_sellers = 1 then 'simple_order'
            when oia.total_items <= 3 and oia.total_sellers = 1 then 'standard_order'
            when oia.total_items <= 5 and oia.total_sellers <= 2 then 'moderate_order'
            when oia.total_items <= 10 and oia.total_sellers <= 3 then 'complex_order'
            else 'very_complex_order'
        end as order_complexity,
        
        -- Cross-seller indicator
        case when oia.total_sellers > 1 then true else false end as is_multi_seller_order,
        
        -- Cross-state shipping indicator (simplified approach)
        case 
            when c.customer_state != 'SP' and oia.seller_states_count > 0 then true 
            else false 
        end as is_cross_state_order,
        
        -- =============================================================================
        -- CUSTOMER BEHAVIOR METRICS
        -- =============================================================================
        -- Customer order analytics
        count(*) over (partition by c.customer_unique_id) as customer_total_orders,
        round(count(*) over () / count(distinct c.customer_unique_id) over (), 2) as avg_orders_per_customer,
        row_number() over (partition by c.customer_unique_id order by d.date_value) as customer_order_sequence,
        
        -- Customer ordering behavior classification
        case 
            when count(*) over (partition by c.customer_unique_id) = 1 then 'single_order_customer'
            when count(*) over (partition by c.customer_unique_id) = 2 then 'two_order_customer'
            when count(*) over (partition by c.customer_unique_id) <= 5 then 'regular_customer'
            when count(*) over (partition by c.customer_unique_id) <= 10 then 'frequent_customer'
            else 'very_frequent_customer'
        end as customer_order_behavior,
        
        -- =============================================================================
        -- FINANCIAL METRICS
        -- =============================================================================
        oia.total_item_value,
        oia.total_freight_value,
        oia.total_payment_value,
        round(oia.total_item_value + oia.total_freight_value, 2) as total_order_value,
        
        -- Price analysis
        oia.avg_item_price,
        oia.max_item_price,
        oia.min_item_price,
        round(oia.max_item_price - oia.min_item_price, 2) as price_spread,
        
        -- Financial ratios
        round(oia.total_freight_value / nullif(oia.total_item_value, 0) * 100, 2) as freight_to_items_ratio_pct,
        round(oia.total_payment_value / nullif(oia.total_item_value + oia.total_freight_value, 0) * 100, 2) as payment_coverage_pct,
        
        -- Value classification
        case 
            when round(oia.total_item_value + oia.total_freight_value, 2) >= 500 then 'premium_order'
            when round(oia.total_item_value + oia.total_freight_value, 2) >= 200 then 'high_value_order'
            when round(oia.total_item_value + oia.total_freight_value, 2) >= 100 then 'medium_value_order'
            when round(oia.total_item_value + oia.total_freight_value, 2) >= 50 then 'standard_order'
            else 'low_value_order'
        end as order_value_tier,
        
        -- =============================================================================
        -- PAYMENT BEHAVIOR METRICS
        -- =============================================================================
        oia.avg_installments,
        oia.max_installments,
        oia.payment_methods_used,
        
        -- Payment behavior classification
        case 
            when oia.max_installments = 1 then 'single_payment'
            when oia.max_installments <= 3 then 'short_installment'
            when oia.max_installments <= 6 then 'medium_installment'
            when oia.max_installments <= 12 then 'long_installment'
            else 'extended_installment'
        end as payment_behavior_type,
        
        -- =============================================================================
        -- PHYSICAL CHARACTERISTICS
        -- =============================================================================
        oia.avg_product_weight,
        oia.total_order_weight,
        oia.avg_product_volume,
        
        -- Shipping complexity based on weight and volume
        case 
            when oia.total_order_weight <= 1000 then 'light_shipment'
            when oia.total_order_weight <= 5000 then 'medium_shipment'
            when oia.total_order_weight <= 15000 then 'heavy_shipment'
            else 'very_heavy_shipment'
        end as shipping_weight_category,
        
        -- =============================================================================
        -- REVIEW & SATISFACTION METRICS
        -- =============================================================================
        oia.avg_review_score,
        oia.total_reviews,
        oia.positive_reviews,
        oia.negative_reviews,
        
        -- Satisfaction classification
        case 
            when oia.avg_review_score >= 4.5 then 'highly_satisfied'
            when oia.avg_review_score >= 4.0 then 'satisfied'
            when oia.avg_review_score >= 3.0 then 'neutral'
            when oia.avg_review_score >= 2.0 then 'dissatisfied'
            when oia.avg_review_score > 0 then 'highly_dissatisfied'
            else 'no_feedback'
        end as satisfaction_level,
        
        -- =============================================================================
        -- GEOGRAPHIC CLASSIFICATION
        -- =============================================================================
        case 
            when c.customer_state in ('SP', 'RJ', 'MG', 'ES') then 'southeast'
            when c.customer_state in ('RS', 'SC', 'PR') then 'south'
            when c.customer_state in ('GO', 'MT', 'MS', 'DF') then 'center_west'
            when c.customer_state in ('BA', 'SE', 'AL', 'PE', 'PB', 'RN', 'CE', 'PI', 'MA') then 'northeast'
            when c.customer_state in ('AC', 'AM', 'AP', 'PA', 'RO', 'RR', 'TO') then 'north'
            else 'unknown'
        end as customer_region,
        
        -- Market tier classification
        case 
            when c.customer_state = 'SP' then 'tier_1_sao_paulo'
            when c.customer_state in ('RJ', 'MG') then 'tier_1_major_southeast'
            when c.customer_state in ('RS', 'PR', 'SC') then 'tier_2_south'
            when c.customer_state in ('BA', 'GO', 'PE') then 'tier_2_major_regional'
            else 'tier_3_emerging'
        end as market_tier,
        
        -- =============================================================================
        -- BUSINESS PERFORMANCE INDICATORS
        -- =============================================================================
        -- Order efficiency metrics
        round(oia.total_items / nullif(oia.total_sellers, 0), 2) as items_per_seller_ratio,
        round((oia.total_item_value + oia.total_freight_value) / nullif(oia.total_items, 0), 2) as value_per_item,
        
        -- Logistical complexity score (1-10 scale)
        least(10, greatest(1, 
            1 + 
            (case when oia.total_sellers > 1 then 2 else 0 end) +
            (case when oia.seller_states_count > 1 then 2 else 0 end) +
            (case when oia.total_items > 5 then 2 else 0 end) +
            (case when oia.total_order_weight > 10000 then 2 else 0 end) +
            (case when oia.total_categories > 3 then 1 else 0 end)
        )) as logistics_complexity_score,
        
        -- =============================================================================
        -- AUDIT FIELDS
        -- =============================================================================
        current_datetime() as last_updated_timestamp
        
    from order_items_aggregated oia
    inner join {{ ref('dim_orders') }} o on oia.order_sk = o.order_sk
    inner join {{ ref('dim_date') }} d on oia.order_date_sk = d.date_sk
    inner join {{ ref('dim_customer') }} c on oia.customer_sk = c.customer_sk
)

select * from orders_analytics_obt
