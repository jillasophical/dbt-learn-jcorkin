
    with source as 
    (select
        id as payment_id
        , orderid as order_id
        , convert_timezone('America/New_York', 'UTC', _batched_at) as batched_timestamp
        , created as created_date
        , paymentmethod as payment_method
        , status as payment_status
        , amount/100 as payment_amount
       
    
    from raw.stripe.payment

)

select * from source