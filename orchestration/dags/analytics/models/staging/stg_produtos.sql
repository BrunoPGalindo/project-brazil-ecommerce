WITH source AS (
    SELECT * FROM {{ source('olist_dw', 'source_products_dataset') }}
)
SELECT
    product_id,
    product_category_name,
    product_weight_g,
    product_length_cm
FROM source