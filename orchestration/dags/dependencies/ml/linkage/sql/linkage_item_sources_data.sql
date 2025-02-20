with
    k as (
        select ie.item_id, ie.name_embedding
        from `{{ bigquery_ml_preproc_dataset }}.item_embedding_reduced_32` ie
        inner join
            `{{ bigquery_analytics_dataset }}.global_offer` go
            on go.item_id = ie.item_id
        where go.offer_product_id is not null
        qualify
            row_number() over (
                partition by ie.item_id order by ie.reduction_method desc
            )
            = 1

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
        go.offer_subcategory_id
    FROM
        `{{ bigquery_analytics_dataset }}.global_offer` go
    where go.offer_product_id is not null
),sources as (
SELECT
    CASE WHEN o.item_id like 'link-%' THEN CONCAT('offer-', o.offer_id) ELSE o.item_id END AS item_id,
    z.embedding,
    o.offer_name,
    o.offer_description,
    o.performer,
    o.offer_subcategory_id
FROM
    offers o
INNER JOIN z on z.item_id = o.item_id
)
select * from sources
QUALIFY ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY performer DESC) = 1
order by RAND()
