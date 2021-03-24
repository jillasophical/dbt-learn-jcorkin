
 with source as 
    (select
        id as order_id
        , user_id as customer_id
        , order_date
        , status as order_status
        , convert_timezone('America/New_York', 'UTC', _etl_loaded_at) as etl_loaded_timestamp
        
       
    
    from raw.jaffle_shop.orders

)

select * from source