with
    payments as (select * from {{ ref("stg_stripe__payments") }}),

    total_amount_paid as (
        select
            order_id,
            max(created) as payment_finalized_date,
            sum(amount) as total_amount_paid
        from payments
        where payment_status <> 'fail'
        group by 1
    )

select 
    p.payment_id,
    p.order_id,
    p.paymentmethod,
    p.payment_status,
    p.amount,
    p.created,
    t.payment_finalized_date,
    t.total_amount_paid,
    p._batched_at
from payments p
left join total_amount_paid t on p.order_id = t.order_id
order by p.order_id
