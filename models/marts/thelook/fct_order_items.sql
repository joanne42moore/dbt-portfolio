with order_items as (
    select * from {{ ref('stg_thelook__order_items') }}
),

orders as (
    select * from {{ ref('stg_thelook__orders') }}
),

users as (
    select * from {{ ref('stg_thelook__users') }}
),

products as (
    select * from {{ ref('stg_thelook__products') }}
)

select
    -- Identifiers
    oi.order_item_id,
    oi.order_id,
    oi.user_id,
    oi.product_id,
    oi.inventory_item_id,

    -- Timestamps
    o.created_at as order_created_at,
    oi.created_at as item_created_at,
    oi.shipped_at,
    oi.delivered_at,
    oi.returned_at,

    -- Product attributes
    p.category as product_category,
    p.brand as product_brand,
    p.department as product_department,

    -- Customer attributes
    u.traffic_source,
    u.country,
    u.state,
    u.city,
    {{ age_cohort_bucket(u.age) }} as age_cohort,

    -- Revenue & status
    oi.status as order_item_status,
    oi.sale_price,
    case when oi.status = 'Returned'  then 1 else 0 end as is_returned,
    case when oi.status = 'Cancelled' then 1 else 0 end as is_cancelled,
    case
        when oi.status in ('Cancelled', 'Returned') then 0
        else 1
    end as is_completed,
    case
        when oi.status in ('Cancelled', 'Returned') then oi.sale_price
        else 0
    end as revenue_leakage_amount,
    case
        when oi.status in ('Cancelled', 'Returned') then 0
        else oi.sale_price
    end as net_revenue_amount
from order_items oi
left join orders   o on oi.order_id = o.order_id
left join users    u on oi.user_id = u.user_id
left join products p on oi.product_id = p.product_id;

