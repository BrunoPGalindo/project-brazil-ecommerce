WITH source AS (
    SELECT * FROM {{ source('olist_dw', 'source_order_items_dataset') }}
)
SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    price,
    freight_value
FROM source