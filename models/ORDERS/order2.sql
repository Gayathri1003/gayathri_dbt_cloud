{{

    config (
        materialized = 'view'
    )
}}

SELECT
    order_id::INT AS order_id,
    customer_id::INT AS customer_id,
    order_date::DATE AS order_date,
    EXTRACT(YEAR FROM order_date) AS order_year,
    LOWER(status) AS status,
    total_amount::NUMERIC(10,2) AS total_amount,
    payment_method::STRING AS payment_method
FROM 
    {{ source('shop_src', 'orders') }}
WHERE status != 'cancelled'


