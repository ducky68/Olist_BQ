-- Total Sales for the Top 10 Most Sold Products
-- Query using warehouse fact and dimension tables

with product_sales_summary as (
    select 
        p.product_id,
        p.product_category_name,
        p.product_category_name_english,
        count(*) as total_units_sold,
        sum(f.price) as total_revenue,
        sum(f.payment_value) as total_payment_allocated,
        sum(f.freight_value) as total_freight,
        count(distinct f.order_id) as distinct_orders,
        avg(f.price) as avg_unit_price,
        min(f.price) as min_price,
        max(f.price) as max_price
    from 
        `project-olist-470307.dbt_olist_stg_dwh.fact_order_items` f
    inner join 
        `project-olist-470307.dbt_olist_stg_dwh.dim_product` p 
        on f.product_sk = p.product_sk
    group by 
        p.product_id,
        p.product_category_name,
        p.product_category_name_english
),

top_10_most_sold as (
    select 
        *,
        row_number() over (order by total_units_sold desc) as sales_rank
    from product_sales_summary
    order by total_units_sold desc
    limit 10
)

select 
    sales_rank,
    product_id,
    product_category_name as category_portuguese,
    product_category_name_english as category_english,
    total_units_sold,
    round(total_revenue, 2) as total_sales_revenue,
    round(total_payment_allocated, 2) as total_payment_received,
    round(total_freight, 2) as total_freight_charges,
    distinct_orders,
    round(avg_unit_price, 2) as average_unit_price,
    round(min_price, 2) as minimum_price,
    round(max_price, 2) as maximum_price,
    -- Calculate additional metrics
    round(total_revenue / total_units_sold, 2) as revenue_per_unit,
    round(total_payment_allocated / total_units_sold, 2) as payment_per_unit,
    round(total_revenue / distinct_orders, 2) as revenue_per_order
from top_10_most_sold
order by sales_rank;

-- =====================================================
-- Total Unique Orders and Customers with Spending (All Years)
-- =====================================================

with orders_customers_all_years as (
    select 
        f.order_id,
        f.customer_sk,
        c.customer_id,
        c.customer_city,
        c.customer_state,
        d.year,
        d.month,
        d.date_value,
        sum(f.price) as order_revenue,
        sum(f.payment_value) as order_payment_allocated,
        sum(f.freight_value) as order_freight,
        count(*) as items_in_order
    from 
        `project-olist-470307.dbt_olist_stg_dwh.fact_order_items` f
    inner join 
        `project-olist-470307.dbt_olist_stg_dwh.dim_customer` c 
        on f.customer_sk = c.customer_sk
    inner join 
        `project-olist-470307.dbt_olist_stg_dwh.dim_date` d 
        on f.order_date_sk = d.date_sk
    group by 
        f.order_id,
        f.customer_sk,
        c.customer_id,
        c.customer_city,
        c.customer_state,
        d.year,
        d.month,
        d.date_value
),

summary_all_years as (
    select 
        count(distinct order_id) as total_unique_orders,
        count(distinct customer_id) as total_unique_customers,
        sum(order_revenue) as total_revenue_spent,
        sum(order_payment_allocated) as total_payments_allocated,
        sum(order_freight) as total_freight_spent,
        sum(items_in_order) as total_items_purchased,
        avg(order_revenue) as avg_order_value,
        min(order_revenue) as min_order_value,
        max(order_revenue) as max_order_value,
        count(distinct customer_state) as states_with_orders,
        count(distinct customer_city) as cities_with_orders
    from orders_customers_all_years
)

select 
    'All Years Business Summary' as report_title,
    total_unique_orders,
    total_unique_customers,
    round(total_revenue_spent, 2) as total_revenue_spent,
    round(total_payments_allocated, 2) as total_payments_allocated,
    round(total_freight_spent, 2) as total_freight_spent,
    total_items_purchased,
    round(avg_order_value, 2) as average_order_value,
    round(min_order_value, 2) as minimum_order_value,
    round(max_order_value, 2) as maximum_order_value,
    states_with_orders,
    cities_with_orders,
    -- Calculate additional business metrics with zero division protection
    round(
        CASE 
            WHEN total_unique_customers > 0 THEN total_revenue_spent / total_unique_customers 
            ELSE 0 
        END, 2
    ) as revenue_per_customer,
    round(
        CASE 
            WHEN total_unique_orders > 0 THEN total_revenue_spent / total_unique_orders 
            ELSE 0 
        END, 2
    ) as revenue_per_order,
    round(
        CASE 
            WHEN total_unique_customers > 0 THEN total_unique_orders / CAST(total_unique_customers AS FLOAT64) 
            ELSE 0 
        END, 2
    ) as orders_per_customer
from summary_all_years;

-- =====================================================
-- Total Number of Orders by State
-- =====================================================

with orders_by_state as (
    select 
        c.customer_state,
        count(distinct f.order_id) as total_orders,
        count(*) as total_order_items,
        sum(f.price) as total_revenue,
        sum(f.payment_value) as total_payments,
        sum(f.freight_value) as total_freight,
        count(distinct c.customer_id) as unique_customers,
        count(distinct c.customer_city) as cities_in_state,
        avg(f.price) as avg_item_price,
        min(d.date_value) as first_order_date,
        max(d.date_value) as last_order_date
    from 
        `project-olist-470307.dbt_olist_stg_dwh.fact_order_items` f
    inner join 
        `project-olist-470307.dbt_olist_stg_dwh.dim_customer` c 
        on f.customer_sk = c.customer_sk
    inner join 
        `project-olist-470307.dbt_olist_stg_dwh.dim_date` d 
        on f.order_date_sk = d.date_sk
    group by 
        c.customer_state
),

state_rankings as (
    select 
        *,
        row_number() over (order by total_orders desc) as order_rank,
        row_number() over (order by total_revenue desc) as revenue_rank,
        row_number() over (order by unique_customers desc) as customer_rank
    from orders_by_state
)

select 
    order_rank,
    customer_state as state_code,
    total_orders,
    total_order_items,
    round(total_revenue, 2) as total_revenue,
    round(total_payments, 2) as total_payments,
    round(total_freight, 2) as total_freight,
    unique_customers,
    cities_in_state,
    round(avg_item_price, 2) as avg_item_price,
    first_order_date,
    last_order_date,
    revenue_rank,
    customer_rank,
    -- Calculate state performance metrics
    round(
        CASE 
            WHEN unique_customers > 0 THEN total_orders / CAST(unique_customers AS FLOAT64)
            ELSE 0 
        END, 2
    ) as orders_per_customer,
    round(
        CASE 
            WHEN total_orders > 0 THEN total_revenue / total_orders
            ELSE 0 
        END, 2
    ) as revenue_per_order,
    round(
        CASE 
            WHEN unique_customers > 0 THEN total_revenue / unique_customers
            ELSE 0 
        END, 2
    ) as revenue_per_customer
from state_rankings
order by total_orders desc;

-- =====================================================
-- Total Number of Sales from Start to End (All Time Summary)
-- =====================================================

with all_time_summary as (
    select 
        count(*) as total_sales_transactions,
        count(distinct f.order_id) as total_unique_orders,
        count(distinct c.customer_id) as total_unique_customers,
        count(distinct p.product_id) as total_unique_products,
        count(distinct c.customer_state) as total_states_served,
        count(distinct c.customer_city) as total_cities_served,
        sum(f.price) as total_revenue,
        sum(f.payment_value) as total_payments_received,
        sum(f.freight_value) as total_freight_charges,
        avg(f.price) as avg_item_price,
        min(d.date_value) as business_start_date,
        max(d.date_value) as business_end_date,
        min(d.year) as first_year,
        max(d.year) as last_year
    from 
        `project-olist-470307.dbt_olist_stg_dwh.fact_order_items` f
    inner join 
        `project-olist-470307.dbt_olist_stg_dwh.dim_customer` c 
        on f.customer_sk = c.customer_sk
    inner join 
        `project-olist-470307.dbt_olist_stg_dwh.dim_product` p 
        on f.product_sk = p.product_sk
    inner join 
        `project-olist-470307.dbt_olist_stg_dwh.dim_date` d 
        on f.order_date_sk = d.date_sk
),

business_duration as (
    select 
        *,
        DATE_DIFF(business_end_date, business_start_date, DAY) as total_business_days,
        (last_year - first_year + 1) as total_business_years
    from all_time_summary
)

select 
    'All Time Business Summary' as report_title,
    total_sales_transactions,
    total_unique_orders,
    total_unique_customers,
    total_unique_products,
    total_states_served,
    total_cities_served,
    round(total_revenue, 2) as total_revenue,
    round(total_payments_received, 2) as total_payments_received,
    round(total_freight_charges, 2) as total_freight_charges,
    round(avg_item_price, 2) as avg_item_price,
    business_start_date,
    business_end_date,
    first_year,
    last_year,
    total_business_days,
    total_business_years,
    -- Calculate comprehensive business metrics
    round(
        CASE 
            WHEN total_unique_customers > 0 THEN total_sales_transactions / CAST(total_unique_customers AS FLOAT64)
            ELSE 0 
        END, 2
    ) as avg_transactions_per_customer,
    round(
        CASE 
            WHEN total_unique_orders > 0 THEN total_sales_transactions / CAST(total_unique_orders AS FLOAT64)
            ELSE 0 
        END, 2
    ) as avg_items_per_order,
    round(
        CASE 
            WHEN total_unique_customers > 0 THEN total_revenue / total_unique_customers
            ELSE 0 
        END, 2
    ) as revenue_per_customer,
    round(
        CASE 
            WHEN total_unique_orders > 0 THEN total_revenue / total_unique_orders
            ELSE 0 
        END, 2
    ) as revenue_per_order,
    round(
        CASE 
            WHEN total_business_days > 0 THEN total_sales_transactions / CAST(total_business_days AS FLOAT64)
            ELSE 0 
        END, 2
    ) as avg_sales_per_day,
    round(
        CASE 
            WHEN total_business_days > 0 THEN total_revenue / total_business_days
            ELSE 0 
        END, 2
    ) as avg_revenue_per_day
from business_duration;

-- =====================================================
-- Total Number of Sales for Toys
-- =====================================================

with toys_sales_summary as (
    select 
        p.product_category_name,
        p.product_category_name_english,
        count(*) as total_toy_sales_transactions,
        count(distinct f.order_id) as total_toy_orders,
        count(distinct c.customer_id) as total_toy_customers,
        count(distinct p.product_id) as total_toy_products,
        count(distinct c.customer_state) as states_buying_toys,
        count(distinct c.customer_city) as cities_buying_toys,
        sum(f.price) as total_toy_revenue,
        sum(f.payment_value) as total_toy_payments,
        sum(f.freight_value) as total_toy_freight,
        avg(f.price) as avg_toy_price,
        min(f.price) as min_toy_price,
        max(f.price) as max_toy_price,
        min(d.date_value) as first_toy_sale_date,
        max(d.date_value) as last_toy_sale_date
    from 
        `project-olist-470307.dbt_olist_stg_dwh.fact_order_items` f
    inner join 
        `project-olist-470307.dbt_olist_stg_dwh.dim_product` p 
        on f.product_sk = p.product_sk
    inner join 
        `project-olist-470307.dbt_olist_stg_dwh.dim_customer` c 
        on f.customer_sk = c.customer_sk
    inner join 
        `project-olist-470307.dbt_olist_stg_dwh.dim_date` d 
        on f.order_date_sk = d.date_sk
    where 
        -- Filter for toys in both Portuguese and English category names
        (
            lower(p.product_category_name) like '%brinquedo%'
            or lower(p.product_category_name) like '%toy%'
            or lower(p.product_category_name_english) like '%toy%'
            or lower(p.product_category_name_english) like '%game%'
            or p.product_category_name_english = 'toys'
        )
    group by 
        p.product_category_name,
        p.product_category_name_english
),

toys_aggregated as (
    select 
        sum(total_toy_sales_transactions) as total_toy_sales_transactions,
        sum(total_toy_orders) as total_toy_orders,
        count(distinct total_toy_customers) as total_unique_toy_customers,
        sum(total_toy_products) as total_toy_products,
        max(states_buying_toys) as states_buying_toys,
        max(cities_buying_toys) as cities_buying_toys,
        sum(total_toy_revenue) as total_toy_revenue,
        sum(total_toy_payments) as total_toy_payments,
        sum(total_toy_freight) as total_toy_freight,
        avg(avg_toy_price) as overall_avg_toy_price,
        min(min_toy_price) as lowest_toy_price,
        max(max_toy_price) as highest_toy_price,
        min(first_toy_sale_date) as first_toy_sale_date,
        max(last_toy_sale_date) as last_toy_sale_date
    from toys_sales_summary
)

select 
    'Toys Sales Summary' as report_title,
    total_toy_sales_transactions,
    total_toy_orders,
    total_unique_toy_customers,
    total_toy_products,
    states_buying_toys,
    cities_buying_toys,
    round(total_toy_revenue, 2) as total_toy_revenue,
    round(total_toy_payments, 2) as total_toy_payments,
    round(total_toy_freight, 2) as total_toy_freight,
    round(overall_avg_toy_price, 2) as avg_toy_price,
    round(lowest_toy_price, 2) as lowest_toy_price,
    round(highest_toy_price, 2) as highest_toy_price,
    first_toy_sale_date,
    last_toy_sale_date,
    -- Calculate toy-specific business metrics
    round(
        CASE 
            WHEN total_unique_toy_customers > 0 THEN total_toy_sales_transactions / CAST(total_unique_toy_customers AS FLOAT64)
            ELSE 0 
        END, 2
    ) as avg_toy_purchases_per_customer,
    round(
        CASE 
            WHEN total_toy_orders > 0 THEN total_toy_sales_transactions / CAST(total_toy_orders AS FLOAT64)
            ELSE 0 
        END, 2
    ) as avg_toys_per_order,
    round(
        CASE 
            WHEN total_unique_toy_customers > 0 THEN total_toy_revenue / total_unique_toy_customers
            ELSE 0 
        END, 2
    ) as revenue_per_toy_customer,
    round(
        CASE 
            WHEN total_toy_orders > 0 THEN total_toy_revenue / total_toy_orders
            ELSE 0 
        END, 2
    ) as revenue_per_toy_order,
    round(
        CASE 
            WHEN DATE_DIFF(last_toy_sale_date, first_toy_sale_date, DAY) > 0 
            THEN total_toy_sales_transactions / CAST(DATE_DIFF(last_toy_sale_date, first_toy_sale_date, DAY) AS FLOAT64)
            ELSE 0 
        END, 2
    ) as avg_toy_sales_per_day
from toys_aggregated

UNION ALL

-- Show breakdown by toy category
select 
    CONCAT('Category: ', product_category_name_english) as report_title,
    total_toy_sales_transactions,
    total_toy_orders,
    total_toy_customers as total_unique_toy_customers,
    total_toy_products,
    states_buying_toys,
    cities_buying_toys,
    round(total_toy_revenue, 2) as total_toy_revenue,
    round(total_toy_payments, 2) as total_toy_payments,
    round(total_toy_freight, 2) as total_toy_freight,
    round(avg_toy_price, 2) as avg_toy_price,
    round(min_toy_price, 2) as lowest_toy_price,
    round(max_toy_price, 2) as highest_toy_price,
    first_toy_sale_date,
    last_toy_sale_date,
    round(
        CASE 
            WHEN total_toy_customers > 0 THEN total_toy_sales_transactions / CAST(total_toy_customers AS FLOAT64)
            ELSE 0 
        END, 2
    ) as avg_toy_purchases_per_customer,
    round(
        CASE 
            WHEN total_toy_orders > 0 THEN total_toy_sales_transactions / CAST(total_toy_orders AS FLOAT64)
            ELSE 0 
        END, 2
    ) as avg_toys_per_order,
    round(
        CASE 
            WHEN total_toy_customers > 0 THEN total_toy_revenue / total_toy_customers
            ELSE 0 
        END, 2
    ) as revenue_per_toy_customer,
    round(
        CASE 
            WHEN total_toy_orders > 0 THEN total_toy_revenue / total_toy_orders
            ELSE 0 
        END, 2
    ) as revenue_per_toy_order,
    round(
        CASE 
            WHEN DATE_DIFF(last_toy_sale_date, first_toy_sale_date, DAY) > 0 
            THEN total_toy_sales_transactions / CAST(DATE_DIFF(last_toy_sale_date, first_toy_sale_date, DAY) AS FLOAT64)
            ELSE 0 
        END, 2
    ) as avg_toy_sales_per_day
from toys_sales_summary
order by total_toy_sales_transactions desc;