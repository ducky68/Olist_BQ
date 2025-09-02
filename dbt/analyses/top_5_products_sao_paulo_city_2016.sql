-- Top 5 Products Sold in São Paulo CITY in 2016
-- Focused analysis on São Paulo city specifically

with sao_paulo_city_sales_2016 as (
    select 
        f.product_sk,
        p.product_id,
        p.product_category_name,
        p.product_category_name_english,
        count(*) as units_sold,
        sum(f.price) as total_revenue,
        sum(f.payment_value) as total_payment_allocated,
        count(distinct f.order_id) as distinct_orders
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
        -- Filter for São Paulo city specifically
        and (
            lower(trim(c.customer_city)) like '%sao paulo%' 
            or lower(trim(c.customer_city)) like '%são paulo%'
            or lower(trim(c.customer_city)) = 'sp'
        )
        -- Filter for 2016
        and d.year = 2016
    group by 
        f.product_sk,
        p.product_id,
        p.product_category_name,
        p.product_category_name_english
)

select 
    row_number() over (order by units_sold desc) as rank,
    product_id,
    product_category_name as category_portuguese,
    product_category_name_english as category_english,
    units_sold,
    round(total_revenue, 2) as total_revenue,
    round(total_payment_allocated, 2) as total_payment,
    distinct_orders,
    round(total_revenue / units_sold, 2) as avg_revenue_per_unit,
    round(total_payment_allocated / units_sold, 2) as avg_payment_per_unit
from sao_paulo_city_sales_2016
order by units_sold desc
limit 5;
