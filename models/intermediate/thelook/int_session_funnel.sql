with events as (
    select *
    from {{ ref('stg_thelook__events') }}
    where session_id is not null
),

sessions as (
    select
        session_id,
        any_value(user_id) as user_id,
        any_value(traffic_source) as traffic_source,
        min(created_at) as session_start_at,

        -- Funnel flags
        countif(event_type = 'product') > 0 as has_product_view,
        countif(event_type = 'cart') > 0 as has_add_to_cart,
        countif(event_type = 'purchase') > 0 as has_purchase
    from events
    group by session_id
)

select
    session_id,
    user_id,
    traffic_source,
    session_start_at,
    has_product_view,
    has_add_to_cart,
    has_purchase
from sessions

