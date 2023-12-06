WITH offer_booking AS (
    SELECT
        item_id,
        sum(booking_cnt) as booking_cnt
    FROM
        `{{ bigquery_analytics_dataset }}.enriched_offer_data`
    GROUP BY
        1
)
SELECT
    ic.cluster as cluster_id,
    ic.cluster_name as semantic_category,
    semantic_cluster_id,
    ic.x_cluster,
    ic.y_cluster,
    ic.item_id,
    ic.semantic_encoding,
    if(ei.offer_name = 'None', '', ei.offer_name) as offer_name,
    if(
        ei.offer_description = 'None',
        '',
        ei.offer_description
    ) as offer_description,
    ob.booking_cnt
FROM
    `{{ bigquery_clean_dataset }}.item_clusters` ic
    LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_item_metadata` ei on ei.item_id = ic.item_id
    LEFT JOIN offer_booking ob on ob.item_id = ic.item_id QUALIFY ROW_NUMBER() OVER (
        PARTITION BY ic.item_id
        ORDER BY
            ic.cluster ASC
    ) = 1