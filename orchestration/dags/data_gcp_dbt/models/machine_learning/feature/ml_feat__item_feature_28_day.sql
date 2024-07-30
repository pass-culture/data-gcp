WITH item_count AS (
    SELECT
        item_id,
        count(distinct offer_id) as total_offers
    FROM {{ ref("int_applicative__offer") }}
    GROUP BY item_id
),

embeddings AS (
    SELECT
        item_id,
        semantic_content_embedding as semantic_content_embedding
    FROM {{ ref('ml_feat__item_embedding') }}
),

avg_embedding AS (
    SELECT
        item_id,
        avg(cast(e as float64)) AS avg_semantic_embedding
    FROM
        embeddings, UNNEST(SPLIT(SUBSTR(semantic_content_embedding,2,LENGTH(semantic_content_embedding) - 2))) e
    GROUP BY item_id
),

booking_numbers AS (
    SELECT
        item_id,
        SUM(IF(
            booking_creation_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY), 1, 0
        )) AS booking_number_last_7_days,
        SUM(IF(
            booking_creation_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY), 1, 0
        )) AS booking_number_last_14_days,
        SUM(IF(
            booking_creation_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY), 1, 0
        )) AS booking_number_last_28_days
    FROM {{ ref('int_applicative__booking') }} booking
    INNER JOIN {{ ref('int_applicative__stock') }} stock ON booking.stock_id = stock.stock_id
    INNER JOIN  {{ ref('int_applicative__offer') }}  offer ON stock.offer_id = offer.offer_id
    WHERE
        booking_creation_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 28 DAY)
        AND NOT booking_is_cancelled
    GROUP BY
        item_id
),

item_clusters AS (
    SELECT
        ic.item_id,
        ANY_VALUE(ic.semantic_cluster_id) as cluster_id,
        ANY_VALUE(it.semantic_cluster_id) as topic_id -- TODO: temporary solution, should be removed after the refactor of topics logics.
    FROM {{ source('ml_preproc', 'default_item_cluster') }} ic
    LEFT JOIN {{ source('ml_preproc', 'unconstrained_item_cluster') }} it on it.item_id = ic.item_id
    GROUP BY 1
)


SELECT
    ic.item_id,
    ic.total_offers,
    ae.avg_semantic_embedding,
    bn.booking_number_last_7_days,
    bn.booking_number_last_14_days,
    bn.booking_number_last_28_days,
    icc.cluster_id,
    icc.topic_id
FROM item_count ic
LEFT JOIN avg_embedding ae on ae.item_id = ic.item_id
LEFT JOIN booking_numbers bn on bn.item_id = ic.item_id
LEFT JOIN item_clusters icc on ic.item_id = icc.item_id
