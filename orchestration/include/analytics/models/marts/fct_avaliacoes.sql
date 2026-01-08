SELECT
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_creation_date
FROM {{ ref('stg_avaliacoes_pedidos') }}