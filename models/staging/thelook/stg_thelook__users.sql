with 

source as (

    select * from {{ source('thelook', 'users') }}

),

renamed as (

    select

        -- Primary key
        id as user_id,

        -- Personal information
        first_name,
        last_name,
        email,
        age,
        gender,

        -- Location
        state,
        street_address,
        postal_code,
        city,
        country,
        latitude,
        longitude,
        user_geom as geography_point,

        -- User attributes
        traffic_source,
        created_at

    from source

)

select * from renamed