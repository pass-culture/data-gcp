WITH k AS (
    SELECT
        ie.item_id,
        ie.semantic_content_hybrid_embedding as semantic_content_embedding,
    FROM
        `{{ bigquery_clean_dataset }}.item_embeddings` ie
        INNER JOIN `{{ bigquery_analytics_dataset }}.recommendable_items_raw` ri on ri.item_id = ie.item_id QUALIFY ROW_NUMBER() OVER (
            PARTITION BY item_id
            ORDER by
                extraction_date DESC
        ) = 1
),
z AS (
    SELECT
        item_id,
        ARRAY(
            SELECT
                cast(e as float64)
            FROM
                UNNEST(
                    SPLIT(
                        SUBSTR(
                            semantic_content_embedding,
                            2,
                            LENGTH(semantic_content_embedding) - 2
                        )
                    )
                ) e
        ) AS embedding,
    FROM
        k
)
SELECT
    item_id,
    embedding
FROM
    z