WITH source AS (
    SELECT * FROM {{ source('olist_dw', 'source_sellers_dataset') }}
)

SELECT
    seller_id,
    seller_city,
    seller_state
FROM source