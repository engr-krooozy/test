
with staging as (

    select * from {{ ref('stg_savings_plans') }}

)

select * from staging
