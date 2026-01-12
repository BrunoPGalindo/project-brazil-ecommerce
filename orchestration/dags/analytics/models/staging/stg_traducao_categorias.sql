WITH source AS (
    SELECT * FROM {{ source('olist_dw', 'source_product_category_name_translation_dataset') }}
)
SELECT
    product_category_name,
    product_category_name_english
FROM source