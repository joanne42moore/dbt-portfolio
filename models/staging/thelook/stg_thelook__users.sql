with 

source as (

    select * from {{ source('thelook', 'users') }}

),

renamed as (

    select
        id as user_id,
        first_name,
        last_name,
        email,
        age,
        gender,
        state,
        street_address,
        postal_code,
        city,
        country,
        latitude,
        longitude,
        traffic_source,
        created_at,
        user_geom as geography_point

    from source

)

select * from renamed