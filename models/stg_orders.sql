
with orders as (

    select
        id as order_id
        , user_id as customer_id
        , order_date
        , status as order_status
        , convert_timezone('America/New_York', 'UTC', _etl_loaded_at) as etl_loaded_timestamp
        
       
    
    from {{source('jaffle_shop', 'orders')}}

)

, payments as (
  select * from {{ref('stg_payments')}}
)

select orders.*
, payments.payment_amount as payment_amount
from orders
left join payments
    on orders.order_id = payments.order_id
where payments.payment_status ilike 'success'