with sessions as (
    select *
    from {{ ref('int_session_funnel') }}
    where has_product_view = true
),

traffic_source_funnel as (
    select
        traffic_source,

        -- Volume
        count(*) as total_sessions,

        -- Progression through funnel
        countif(has_add_to_cart) as sessions_with_add_to_cart,
        countif(has_purchase) as sessions_with_purchase,

        -- Rates
        countif(has_add_to_cart) * 1.0 / nullif(count(*), 0) as add_to_cart_rate,
        countif(has_purchase) * 1.0 / nullif(count(*), 0) as purchase_conversion_rate
    from sessions
    group by traffic_source
)

select
    traffic_source,
    total_sessions,
    sessions_with_add_to_cart,
    sessions_with_purchase,
    add_to_cart_rate,
    purchase_conversion_rate
from traffic_source_funnel
order by total_sessions desc

