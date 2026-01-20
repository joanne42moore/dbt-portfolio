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
        round(cast(cost as numeric), 2) as cost, -- cast as numeric to prevent floating point errors
        round(cast(retail_price as numeric), 2) as retail_price -- cast as numeric to prevent floating point errors

    from source

)

select * from renamed
where -- Filter out null IDs
    distribution_center_id is not null
    and product_id is not null