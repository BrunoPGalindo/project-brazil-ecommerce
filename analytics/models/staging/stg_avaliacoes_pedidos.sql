WITH source AS (
    SELECT * FROM {{ source('olist_dw', 'source_order_reviews_dataset') }}
)

SELECT
    review_id,
    MAX(order_id) as order_id,
    MAX(review_score) as review_score,
    MAX(review_comment_title) as review_comment_title,
    MAX(review_creation_date) as review_creation_date
FROM source
GROUP BY 1