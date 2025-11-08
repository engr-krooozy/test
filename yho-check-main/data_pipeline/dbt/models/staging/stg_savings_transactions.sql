
with source as (

    select * from {{ source('staging', 'staging_savings_transaction') }}

),

renamed as (

    select
        data:"txn_id"::string as txn_id,
        data:"plan_id"::string as plan_id,
        data:"amount"::numeric(18, 2) as amount,
        data:"currency"::string as currency,
        data:"side"::string as side,
        data:"rate"::numeric(18, 6) as rate,
        data:"txn_timestamp"::timestamp as txn_timestamp,
        data:"updated_at"::timestamp as updated_at,
        data:"deleted_at"::timestamp as deleted_at

    from source

)

select * from renamed
