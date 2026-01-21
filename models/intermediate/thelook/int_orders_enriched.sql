with orders as (
    select * from {{ ref('stg_thelook__orders') }}
),

users as (
    select * from {{ ref('stg_thelook__users') }}
),

order_items as (
    select * from {{ ref('stg_thelook__order_items') }}
)

select
    orders.order_id,
    orders.user_id,
    orders.status as order_status,
    orders.created_at,
    users.country,
    users.state,
    users.city,
    users.traffic_source,
    case
        when users.age < 18 then '<18'
        when users.age < 25 then '18-24'
        when users.age < 35 then '25-34'
        when users.age < 45 then '35-44'
        when users.age < 55 then '45-54'
        when users.age < 65 then '55-64'
        else '65+'
    end as age_cohort, -- Used cohorts to protect PII
    count(order_items.order_item_id) as item_count,
    sum(order_items.sale_price) as order_total
from orders
left join users on orders.user_id = users.user_id
left join order_items on orders.order_id = order_items.order_id
group by
    orders.order_id,
    orders.user_id,
    order_status,
    orders.created_at,
    users.country,
    users.state,
    users.city,
    users.traffic_source,
    age_cohort