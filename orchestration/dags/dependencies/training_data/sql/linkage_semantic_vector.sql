WITH k AS (
    SELECT
        ie.item_id,
        ie.name_embedding,
    FROM
        `{{ bigquery_clean_dataset }}.item_embeddings_reduced_32` ie
    INNER JOIN `{{ bigquery_ml_reco_dataset }}.recommendable_item` ri on ri.item_id = ie.item_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ie.item_id ORDER BY ie.reduction_method DESC) = 1

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
                            name_embedding,
                            2,
                            LENGTH(name_embedding) - 2
                        )
                    )
                ) e
        ) AS embedding,
    FROM
        k
)
SELECT
    z.item_id,
    z.embedding,
    eod.offer_name,
    eod.performer,
FROM
    z
INNER JOIN `{{ bigquery_analytics_dataset }}.enriched_offer_data` eod on eod.item_id = z.item_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY performer DESC) = 1