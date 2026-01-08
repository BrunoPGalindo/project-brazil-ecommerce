WITH source AS (
    SELECT * FROM {{ source('olist_dw', 'source_customers_dataset') }}
)
SELECT
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state
FROM source