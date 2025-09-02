-- =============================================================================
-- DELIVERY ANALYTICS OBT - SIMPLIFIED VERSION
-- =============================================================================
-- Business Purpose: Basic delivery analysis without complex timestamp operations
-- Grain: One row per order item
-- Update Frequency: Daily
-- =============================================================================

{{
  config(
    materialized='table',
    description='Delivery analytics OBT - simplified version without timestamp operations'
  )
}}

with delivery_basic as (
    select 
        -- Order identifiers
        r.order_id,
        r.order_item_id,
        r.revenue_sk as delivery_sk,
        r.customer_sk,
        r.seller_sk,
        r.product_sk,
        
        -- Geographic context
        r.customer_city,
        r.customer_state,
        r.seller_city,
        r.seller_state,
        r.shipping_complexity,
        
        -- Order context
        r.order_date,
        r.order_year,
        r.order_quarter,
        r.order_month,
        r.total_items_in_order,
        r.total_order_value,
        
        -- Product context
        r.product_category_english,
        r.product_weight_category,
        
        -- Financial metrics
        r.item_price,
        r.freight_cost,
        r.allocated_payment,
        
        -- Customer satisfaction
        r.review_score,
        r.satisfaction_level,
        
        -- Order status only (no timestamps for now)
        o.order_status,
        
        -- Simple flags
        case when o.order_status = 'delivered' then 1 else 0 end as flag_delivered,
        case when o.order_status = 'shipped' then 1 else 0 end as flag_in_transit,
        case when o.order_status = 'canceled' then 1 else 0 end as flag_canceled,
        
        -- Audit timestamp
        current_datetime() as last_updated_timestamp
        
    from {{ ref('revenue_analytics_obt') }} r
    inner join {{ ref('dim_orders') }} o on r.order_id = o.order_id
)

select * from delivery_basic
