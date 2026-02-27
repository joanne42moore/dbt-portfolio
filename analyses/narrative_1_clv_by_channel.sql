with customers as (
    select *
    from {{ ref('dim_customers') }}
    where traffic_source is not null
),

channel_stats as (
    select
        traffic_source as acquisition_channel,

        -- Customer counts
        count(*) as customer_count,
        countif(lifetime_order_count > 1) as repeat_customers,
        countif(lifetime_order_count = 1) as single_purchase_customers,

        -- Lifetime value metrics
        sum(lifetime_order_value) as total_ltv,
        avg(lifetime_order_value) as avg_ltv_per_customer,

        -- Repeat behavior
        countif(lifetime_order_count > 1) * 1.0 / nullif(count(*), 0) as repeat_purchase_rate,

        -- Order behavior (optional but useful context)
        avg(lifetime_order_count) as avg_orders_per_customer,
        max(lifetime_order_count) as max_orders_for_a_customer
    from customers
    group by traffic_source
)

select
    acquisition_channel,
    customer_count,
    repeat_customers,
    single_purchase_customers,
    total_ltv,
    avg_ltv_per_customer,
    repeat_purchase_rate,
    avg_orders_per_customer,
    max_orders_for_a_customer
from channel_stats
order by avg_ltv_per_customer desc;
