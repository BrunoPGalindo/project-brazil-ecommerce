WITH source AS (
    SELECT * FROM {{ source('olist_dw', 'source_orders_dataset') }}
)
SELECT
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at
FROM source