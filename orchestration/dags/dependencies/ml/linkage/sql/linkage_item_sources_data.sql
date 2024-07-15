WITH k AS (
    SELECT
        ie.item_id,
        ie.name_embedding
    FROM
        `{{ bigquery_clean_dataset }}.item_embeddings_reduced_32` ie
    INNER JOIN `{{ bigquery_analytics_dataset }}.global_offer` go on go.item_id = ie.item_id
    where go.is_active
    and go.is_synchronised is True
    and go.offer_subcategory_id<>'LIVRE_PAPIER'
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
),
offers as (
    SELECT
        go.offer_id,
        go.item_id,
        go.offer_name,
        go.offer_description,
        go.performer,
        go.is_synchronised
    FROM
        `{{ bigquery_analytics_dataset }}.global_offer` go
    where go.is_active
    and go.is_synchronised is True
    and go.offer_subcategory_id<>'LIVRE_PAPIER'
)
SELECT
    CASE WHEN o.item_id like 'link-%' THEN o.offer_id ELSE o.item_id END AS item_id,
    z.embedding,
    o.offer_name,
    o.offer_description,
    o.performer,
FROM
    offers o
INNER JOIN z on z.item_id = o.item_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY performer DESC) = 1