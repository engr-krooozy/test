
with source as (

    select * from {{ source('staging', 'staging_savings_plan') }}

),

renamed as (

    select
        data:"plan_id"::string as plan_id,
        data:"product_type"::string as product_type,
        data:"customer_uid"::string as customer_uid,
        data:"amount"::numeric(18, 2) as amount,
        data:"frequency"::string as frequency,
        data:"start_date"::date as start_date,
        data:"end_date"::date as end_date,
        data:"status"::string as status,
        data:"created_at"::timestamp as created_at,
        data:"updated_at"::timestamp as updated_at,
        data:"deleted_at"::timestamp as deleted_at

    from source

)

select * from renamed
