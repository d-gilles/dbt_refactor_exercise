with

    customers as (select * from {{ ref("stg_jaffle_shop_customers") }}),

    orders as (select * from {{ ref("stg_jaffle_shop_orders") }}),

    payments as (
        select distinct order_id, total_amount_paid, payment_finalized_date
        from {{ ref("int_payments") }}

    ),

    paid_orders as (
        select
            o.order_id,
            o.customer_id,
            o.order_placed_at,
            o.order_status,
            p.total_amount_paid,
            p.payment_finalized_date,
            c.first_name as customer_first_name,
            c.last_name as customer_last_name
        from orders o
        left join payments p on o.order_id = p.order_id
        left join customers c on o.customer_id = c.customer_id
    ),

    customer_orders as (
        select
            c.customer_id,
            min(o.order_placed_at) as first_order_date,
            max(o.order_placed_at) as most_recent_order_date,
            count(o.order_id) as number_of_orders
        from customers c
        left join orders o on o.customer_id = c.customer_id
        group by 1
    ),
    final as (
        select
            p.*,
            row_number() over (order by p.order_id) as transaction_seq,
            row_number() over (
                partition by customer_id order by p.order_id
            ) as customer_sales_seq,
            case
                when  (row_number() over( partition by p.order_id order by  p.order_placed_at)) = 1  then 'new' else 'return'
            end as nvsr,
            x.clv_bad as customer_lifetime_value,
            c.first_order_date as fdos
        from paid_orders p
        left join customer_orders as c using (customer_id)
        left outer join
            (
                select p.order_id, sum(t2.total_amount_paid) as clv_bad
                from paid_orders p
                left join
                    paid_orders t2
                    on p.customer_id = t2.customer_id
                    and p.order_id >= t2.order_id
                group by 1
                order by p.order_id
            ) x
            on x.order_id = p.order_id
        order by order_id
    )

select *
from final
