
with transactions as (

    select * from {{ ref('stg_savings_transactions') }}

),

plans as (

    select * from {{ ref('dim_savings_plans') }}

),


final as (

    select
        t.txn_id,
        t.plan_id,
        p.customer_uid as user_id,
        t.amount,
        t.currency,
        t.side,
        t.rate,
        t.txn_timestamp,
        t.updated_at,
        t.deleted_at
    from transactions t
    left join plans p on t.plan_id = p.plan_id

)

select * from final
