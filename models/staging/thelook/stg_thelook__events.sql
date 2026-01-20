with source as (
    select * from {{ source('thelook', 'events') }}
),

renamed as (
    select
        -- Primary key
        id as event_id,
        
        -- Foreign keys
        user_id,
        
        -- Event attributes
        sequence_number,
        session_id,
        event_type,
        uri,
        
        -- User context
        ip_address,
        browser,
        traffic_source,
        
        -- Location
        city,
        state,
        postal_code,
        
        -- Timestamps
        created_at
        
    from source
)

select * from renamed
where user_id is not null and event_id is not null -- Filter out null IDs