WITH itens AS (
    SELECT * FROM {{ ref('stg_itens_pedidos') }}
),

vendedores AS (
    SELECT * FROM {{ ref('stg_vendedores') }}
)

SELECT
    i.order_id,
    i.product_id,
    i.seller_id,
    i.price,
    i.freight_value,
    v.seller_city,
    v.seller_state
FROM itens i
LEFT JOIN vendedores v ON i.seller_id = v.seller_id