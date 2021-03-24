
with orders as (
  select * from {{ref('src_orders')}}
)
, payments as (
  select * from {{ref('src_stripe_payments')}}
)

select orders.*
, payments.payment_amount as payment_amount
from orders
left join payments
on orders.order_id = payments.order_id
where payments.payment_status ilike 'success'