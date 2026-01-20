with 

source as (

    select * from {{ source('thelook', 'orders') }}

),

renamed as (

    select

        -- IDs
        order_id,
        user_id,

        -- Time stamps
        created_at,
        returned_at,
        shipped_at,
        delivered_at,

        -- Attributes
        status,
        gender,
        num_of_item

    from source

)

select * from renamed