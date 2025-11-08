
with staging as (

    select * from {{ ref('stg_users') }}

)

select * from staging
