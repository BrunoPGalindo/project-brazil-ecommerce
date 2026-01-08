WITH pedidos AS (
    SELECT * FROM {{ ref('stg_pedidos') }}
),

pagamentos AS (
    SELECT 
        order_id,
        SUM(payment_value) as total_pago,
        MAX(payment_installments) as max_parcelas
    FROM {{ ref('stg_pagamentos') }}
    GROUP BY 1
),

clientes AS (
    SELECT * FROM {{ ref('stg_clientes') }}
)

SELECT
    p.order_id,
    p.customer_id,
    p.order_status,
    p.order_purchase_timestamp,
    c.customer_city,
    c.customer_state,
    pay.total_pago,
    pay.max_parcelas
FROM pedidos p
LEFT JOIN pagamentos pay ON p.order_id = pay.order_id
LEFT JOIN clientes c ON p.customer_id = c.customer_id