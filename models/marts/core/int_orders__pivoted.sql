with 
orders as 
(
select * from 
{{ ref('stg_stripe__payments') }}
where payment_status = 'success'
),
orders_pivoted as 
(
select 
    order_id,
{%- set payment_methods=('credit_card','coupon','bank_transfer','gift_card') %}
{% for payment_method in payment_methods %}
    sum(case when paymentmethod = '{{ payment_method }}' then amount else 0 end) as {{payment_method}}_amount
    {%- if not loop.last -%}
    ,
        
    {%- endif %}
{% endfor -%}



 from orders
 group by 1
)
select * from orders_pivoted
order by 1
