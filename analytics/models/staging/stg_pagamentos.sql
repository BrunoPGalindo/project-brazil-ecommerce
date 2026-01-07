WITH source AS (
    SELECT * FROM {{ source('olist_dw', 'source_order_payments_dataset') }}
)
SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value
FROM source