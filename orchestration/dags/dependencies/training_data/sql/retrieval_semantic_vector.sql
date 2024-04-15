WITH k AS (
    SELECT
        ie.item_id,
        ie.hybrid_embedding,
    FROM
        `{{ bigquery_clean_dataset }}.item_embeddings_reduced_16` ie
    INNER JOIN `{{ bigquery_analytics_dataset }}.recommendable_items_raw` ri on ri.item_id = ie.item_id 
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
                            hybrid_embedding,
                            2,
                            LENGTH(hybrid_embedding) - 2
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