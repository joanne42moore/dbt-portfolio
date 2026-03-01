with 

source as (

    select * from {{ source('thelook', 'products') }}

),

renamed as (

    select

        -- Primary key
        id as product_id,

        -- Foreign keys
        distribution_center_id,

        -- Product attributes
        name as product_name,
        category,
        brand,
        department,
        sku,
    
        -- Pricing
        {{ cast_dollars('cost') }} as cost,
        {{ cast_dollars('retail_price') }} as retail_price

    from source

)

select * from renamed
where -- Filter out null IDs
    distribution_center_id is not null
    and product_id is not null