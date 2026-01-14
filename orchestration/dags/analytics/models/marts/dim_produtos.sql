WITH produtos AS (
    SELECT * FROM {{ ref('stg_produtos') }}
),

traducao_categorias AS (
    SELECT * FROM {{ ref('stg_traducao_categorias') }}
)

SELECT
    p.product_id,
    MAX(p.product_category_name) as product_category_name,
    MAX(t.product_category_name_english) as product_category_name_english,
    MAX(p.product_weight_g) as product_weight_g,
    MAX(p.product_length_cm) as product_length_cm
FROM produtos p
LEFT JOIN traducao_categorias t ON p.product_category_name = t.product_category_name
WHERE p.product_id IS NOT NULL -- nao pode conter IDs vazios
GROUP BY p.product_id