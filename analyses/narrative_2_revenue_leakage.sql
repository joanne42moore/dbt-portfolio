with order_items as (
    select *
    from {{ ref('fct_order_items') }}
),

product_category_stats as (
    select
        product_category,

        -- Volume
        count(*) as order_item_count,

        -- Revenue metrics
        sum(sale_price) as gross_revenue,
        sum(net_revenue_amount) as net_revenue,
        sum(revenue_leakage_amount) as revenue_leakage,

        -- Rates
        countif(is_returned = 1) * 1.0 / nullif(count(*), 0) as return_rate,
        countif(is_cancelled = 1) * 1.0 / nullif(count(*), 0) as cancellation_rate,
        sum(revenue_leakage_amount) * 1.0 / nullif(sum(sale_price), 0) as revenue_leakage_rate
    from order_items
    group by product_category
)

select
    product_category,
    order_item_count,
    gross_revenue,
    net_revenue,
    revenue_leakage,
    return_rate,
    cancellation_rate,
    revenue_leakage_rate
from product_category_stats
order by gross_revenue desc

