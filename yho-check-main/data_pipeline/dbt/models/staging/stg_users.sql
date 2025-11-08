
with source as (

    select * from {{ source('staging', 'staging_users') }}

),

renamed as (

    select
        data:"_id"::string as user_id,
        data:"Uid"::string as uid,
        data:"firstName"::string as first_name,
        data:"lastName"::string as last_name,
        data:"occupation"::string as occupation,
        data:"state"::string as state,
        data:"last_updated_at"::timestamp as last_updated_at

    from source

)

select * from renamed
