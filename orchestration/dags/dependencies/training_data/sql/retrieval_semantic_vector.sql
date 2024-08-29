WITH k AS (
    SELECT
        ie.item_id,
        ie.hybrid_embedding,
    FROM
        `{{ bigquery_ml_preproc_dataset }}.item_embedding_reduced_32` ie
    INNER JOIN `{{ bigquery_ml_reco_dataset }}.recommendable_item` ri on ri.item_id = ie.item_id 
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