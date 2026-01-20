with user_orders as (
    select * from {{ ref('int_user_order_history') }}
),

first_order_details as (
    select
        user_id,
        country,
        state,
        city,
        traffic_source,
        age_cohort
    from {{ ref('int_orders_enriched') }}
    qualify row_number() over (partition by user_id order by created_at) = 1
)

select
    user_orders.user_id,
    first_order_details.country,
    first_order_details.state,
    first_order_details.city,
    first_order_details.traffic_source,
    first_order_details.age_cohort,
    user_orders.lifetime_order_count,
    user_orders.lifetime_order_value,
    user_orders.avg_order_value,
    user_orders.first_order_at,
    user_orders.most_recent_order_at
from user_orders
left join first_order_details using (user_id)