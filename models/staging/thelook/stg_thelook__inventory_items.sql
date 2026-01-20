with source as (
    select * from {{ source('thelook', 'inventory_items') }}
),

renamed as (
    select
        -- Primary key
        id as inventory_item_id,
        
        -- Foreign keys
        product_id,
        product_distribution_center_id,
        
        -- Inventory item attributes
        cost,
        
        -- Product attributes (denormalized from products table)
        product_name,
        product_brand,
        product_category,
        product_department,
        product_sku,
        product_retail_price,
        
        -- Timestamps
        created_at,
        sold_at
        
    from source
)

select * from renamed
where -- Filter out null IDs
    inventory_item_id is not null
    and product_distribution_center_id is not null 
    and product_id is not null 