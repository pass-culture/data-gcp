WITH k AS (
    SELECT
        ie.item_id,
        ie.name_embedding
    FROM
        `{{ bigquery_ml_preproc_dataset }}.item_embedding_reduced_32` ie
    INNER JOIN `{{ bigquery_analytics_dataset }}.global_offer` go on go.item_id = ie.item_id
    where go.offer_product_id is null
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
        go.offer_subcategory_id
    FROM
        `{{ bigquery_analytics_dataset }}.global_offer` go
    where go.offer_product_id is null
),
bookings as(
    SELECT
        bo.offer_id,
        SUM(bo.booking_quantity) as booking_count
    FROM offers o
    JOIN
        `{{ bigquery_analytics_dataset }}.global_booking` bo on bo.offer_id=o.offer_id
    group by 1
),
candidates as(
    SELECT
        CASE WHEN o.item_id like 'link-%' THEN CONCAT('offer-', o.offer_id) ELSE o.item_id END AS item_id,
        z.embedding,
        o.offer_name,
        o.offer_description,
        o.performer,
        o.offer_subcategory_id,
        b.booking_count
    FROM
        offers o
    INNER JOIN z on z.item_id = o.item_id
    LEFT JOIN bookings b on b.offer_id = o.offer_id
)
select * from candidates
QUALIFY ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY performer DESC) = 1
order by RAND()
