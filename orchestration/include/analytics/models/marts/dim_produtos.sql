WITH produtos AS (
    SELECT * FROM {{ ref('stg_produtos') }}
),

traducao_categorias AS (
    SELECT * FROM {{ ref('stg_traducao_categorias') }}
)

SELECT
    p.product_id,
    p.product_category_name,
    t.product_category_name_english,
    p.product_weight_g,
    p.product_length_cm
FROM produtos p
LEFT JOIN traducao_categorias t ON p.product_category_name = t.product_category_name