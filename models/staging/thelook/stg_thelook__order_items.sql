with 

source as (

    select * from {{ source('thelook', 'order_items') }}

),

renamed as (

    select

        -- Primary key
        id as order_item_id,

        -- Foreign keys
        order_id,
        user_id,
        product_id,
        inventory_item_id,

        -- Order item attributes
        status,
        sale_price,

        -- Timestamps
        created_at,
        shipped_at,
        delivered_at,
        returned_at

    from source

)

select * from renamed
where -- Filter out null IDs
    inventory_item_id is not null
    and order_id is not null
    and order_item_id is not null
    and product_id is not null
    and user_id is not null