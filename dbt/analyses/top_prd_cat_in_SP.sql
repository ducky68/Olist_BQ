-- Top 5 Products Sold in São Paulo in 2016
-- Analysis using warehouse fact and dimension tables

with sao_paulo_sales_2016 as (
    select 
        f.product_sk,
        p.product_id,
        p.product_category_name,
        p.product_category_name_english,
        c.customer_city,
        c.customer_state,
        d.year,
        d.date_value,
        f.order_id,
        f.order_item_id,
        f.price,
        f.freight_value,
        f.payment_value,
        count(*) as units_sold,
        sum(f.price) as total_revenue,
        sum(f.payment_value) as total_payment_allocated,
        avg(f.price) as avg_price
    from 
        `project-olist-470307.dbt_olist_stg.fact_order_items` f
    inner join 
        `project-olist-470307.dbt_olist_stg.dim_product` p 
        on f.product_sk = p.product_sk
    inner join 
        `project-olist-470307.dbt_olist_stg.dim_customer` c 
        on f.customer_sk = c.customer_sk
    inner join 
        `project-olist-470307.dbt_olist_stg.dim_date` d 
        on f.order_date_sk = d.date_sk
    where 
        -- Filter for São Paulo state
        upper(trim(c.customer_state)) = 'SP'
        -- Filter for 2016
        and d.year = 2016
        -- Additional filter for São Paulo city (optional - includes all SP state)
        -- and lower(trim(c.customer_city)) like '%sao paulo%' or lower(trim(c.customer_city)) like '%são paulo%'
    group by 
        f.product_sk,
        p.product_id,
        p.product_category_name,
        p.product_category_name_english,
        c.customer_city,
        c.customer_state,
        d.year,
        d.date_value,
        f.order_id,
        f.order_item_id,
        f.price,
        f.freight_value,
        f.payment_value
),

product_summary as (
    select 
        product_sk,
        product_id,
        product_category_name,
        product_category_name_english,
        sum(units_sold) as total_units_sold,
        sum(total_revenue) as total_product_revenue,
        sum(total_payment_allocated) as total_product_payment,
        avg(avg_price) as average_unit_price,
        count(distinct order_id) as distinct_orders,
        count(distinct customer_city) as cities_sold_in
    from sao_paulo_sales_2016
    group by 
        product_sk,
        product_id,
        product_category_name,
        product_category_name_english
)

select 
    row_number() over (order by total_units_sold desc) as rank,
    product_id,
    product_category_name as category_portuguese,
    product_category_name_english as category_english,
    total_units_sold,
    total_product_revenue,
    total_product_payment,
    round(average_unit_price, 2) as avg_unit_price,
    distinct_orders,
    cities_sold_in,
    -- Calculate metrics
    round(total_product_revenue / total_units_sold, 2) as revenue_per_unit,
    round(total_product_payment / total_units_sold, 2) as payment_per_unit
from product_summary
order by total_units_sold desc
limit 5;