SELECT
    o.order_id,
    o.customer_id,
    o.payment_method,
    p.payment_category
FROM {{ source('shop_src', 'orders') }} o
LEFT JOIN {{ ref('payment_seed') }} p
ON o.payment_method = p.payment_method