{{
  config(
    materialized='table',
    partition_by={
      'field': 'order_date_sk',
      'data_type': 'int64',
      'range': {
        'start': 20160101,
        'end': 20251231,
        'interval': 100
      }
    },
    cluster_by=['customer_sk', 'seller_sk', 'product_sk']
  )
}}

with order_items_base as (
    select 
        oi.order_id,
        oi.order_item_id,
        oi.product_id,
        oi.seller_id,
        oi.shipping_limit_date,
        oi.price,
        oi.freight_value
    from {{ ref('stg_order_items') }} oi
),

orders_base as (
    select 
        o.order_id,
        o.customer_id,
        o.order_purchase_timestamp,
        o.order_status
    from {{ ref('stg_orders') }} o
),

-- Calculate total order value for proportional allocation
order_totals as (
    select 
        order_id,
        sum(price + freight_value) as total_order_value
    from order_items_base
    group by order_id
),

-- Get payment information aggregated by order
payment_totals as (
    select 
        order_id,
        sum(payment_value) as total_payment_value,
        max(payment_installments) as max_payment_installments  -- Use max for the order
    from {{ ref('stg_order_payments') }}
    group by order_id
),

-- Get review information (one review per order)
reviews_agg as (
    select 
        order_id,
        max(review_score) as review_score,  -- Use max if multiple reviews
        max({{ generate_surrogate_key(['review_id']) }}) as review_sk
    from {{ ref('stg_order_reviews') }}
    group by order_id
),

-- Join all base data
fact_base as (
    select 
        oi.order_id,
        oi.order_item_id,
        oi.product_id,
        oi.seller_id,
        oi.shipping_limit_date,
        oi.price,
        oi.freight_value,
        
        o.customer_id,
        o.order_purchase_timestamp,
        o.order_status,
        
        ot.total_order_value,
        pt.total_payment_value,
        pt.max_payment_installments,
        
        r.review_score,
        r.review_sk
        
    from order_items_base oi
    inner join orders_base o on oi.order_id = o.order_id
    left join order_totals ot on oi.order_id = ot.order_id
    left join payment_totals pt on oi.order_id = pt.order_id
    left join reviews_agg r on oi.order_id = r.order_id
),

-- Add dimension surrogate keys
fact_with_dimensions as (
    select 
        fb.*,
        
        -- Generate fact table surrogate key
        {{ generate_surrogate_key(['fb.order_id', 'fb.order_item_id']) }} as order_item_sk,
        
        -- Dimension surrogate keys
        dc.customer_sk,
        dp.product_sk,
        ds.seller_sk,
        do.order_sk,
        
        -- Geography surrogate keys
        dcg.geolocation_sk as customer_geography_sk,
        dsg.geolocation_sk as seller_geography_sk,
        
        -- Payment surrogate key (first payment for the order)
        dpm.payment_sk,
        
        -- Date surrogate keys
        cast(format_date('%Y%m%d', date(fb.order_purchase_timestamp)) as int64) as order_date_sk,
        case 
            when fb.shipping_limit_date is not null 
            then cast(format_date('%Y%m%d', date(fb.shipping_limit_date)) as int64)
            else null 
        end as shipping_limit_date_sk
        
    from fact_base fb
    
    -- Join with dimension tables to get surrogate keys
    inner join {{ ref('dim_customer') }} dc on fb.customer_id = dc.customer_id
    inner join {{ ref('dim_product') }} dp on fb.product_id = dp.product_id
    inner join {{ ref('dim_seller') }} ds on fb.seller_id = ds.seller_id
    inner join {{ ref('dim_orders') }} do on fb.order_id = do.order_id
    
    -- Geography joins
    left join {{ ref('dim_geolocation') }} dcg on dc.customer_zip_code_prefix = dcg.geolocation_zip_code_prefix
    left join {{ ref('dim_geolocation') }} dsg on ds.seller_zip_code_prefix = dsg.geolocation_zip_code_prefix
    
    -- Payment join (get the first payment record for the order)
    left join (
        select 
            order_id,
            payment_sk,
            row_number() over (partition by order_id order by payment_sequential) as rn
        from {{ ref('dim_payment') }}
    ) dpm on fb.order_id = dpm.order_id and dpm.rn = 1
),

-- Calculate proportional payment allocation
fact_final as (
    select 
        order_item_sk,
        order_id,
        order_item_id,
        order_sk,
        customer_sk,
        product_sk,
        seller_sk,
        payment_sk,
        review_sk,
        order_date_sk,
        shipping_limit_date_sk,
        customer_geography_sk,
        seller_geography_sk,
        price,
        freight_value,
        
        -- Proportional payment allocation
        case 
            when total_order_value > 0 and total_payment_value is not null
            then round(
                ((price + freight_value) / total_order_value) * total_payment_value, 
                2
            )
            else price + freight_value
        end as payment_value,
        
        max_payment_installments as payment_installments,
        review_score,
        
        -- Audit timestamp
        current_datetime() as insertion_timestamp
        
    from fact_with_dimensions
    where customer_sk is not null
      and product_sk is not null
      and seller_sk is not null
      and order_sk is not null
      and order_date_sk is not null
)

select 
    order_item_sk,
    order_id,
    order_item_id,
    order_sk,
    customer_sk,
    product_sk,
    seller_sk,
    payment_sk,
    review_sk,
    order_date_sk,
    shipping_limit_date_sk,
    customer_geography_sk,
    seller_geography_sk,
    price,
    freight_value,
    payment_value,
    payment_installments,
    review_score,
    insertion_timestamp
from fact_final