with orders as (
    select * from {{ ref('int_orders_enriched') }}
)

select
    user_id,
    count(order_id) as lifetime_order_count,
    sum(order_total) as lifetime_order_value,
    round(avg(order_total), 2) as avg_order_value,
    min(created_at) as first_order_at,
    max(created_at) as most_recent_order_at
from orders
group by user_id