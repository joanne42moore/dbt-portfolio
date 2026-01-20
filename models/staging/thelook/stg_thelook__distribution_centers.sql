with source as (
    select * from {{ source('thelook', 'distribution_centers') }}
),

renamed as (
    select
        -- Primary key
        id as distribution_center_id,
        
        -- Distribution center attributes
        name as distribution_center_name,
        
        -- Location
        latitude,
        longitude,
        distribution_center_geom as geography_point
        
    from source
)

select * from renamed
where distribution_center_id is not null  -- Filter out null IDs