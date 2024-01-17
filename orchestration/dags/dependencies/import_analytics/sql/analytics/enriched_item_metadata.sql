WITH offer_booking_information_view AS (
    SELECT
        offer.offer_id,
        COUNT(DISTINCT booking.booking_id) AS count_booking
    FROM
        `{{ bigquery_clean_dataset }}`.applicative_database_offer AS offer
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_stock AS stock ON stock.offer_id = offer.offer_id
        LEFT JOIN `{{ bigquery_clean_dataset }}`.booking AS booking ON stock.stock_id = booking.stock_id
    WHERE booking_is_used
    GROUP BY
        offer_id
), 

item_clusters AS (
    SELECT 
        ic.item_id,  
        ANY_VALUE(ic.semantic_cluster_id) as cluster_id,
        ANY_VALUE(it.topic_id) as topic_id
    FROM `{{ bigquery_clean_dataset }}`.item_clusters ic
    LEFT JOIN `{{ bigquery_clean_dataset }}`.item_topics it on it.item_id = ic.item_id
    GROUP BY 1
),

enriched_items AS (
    SELECT 
        offer.*,
        offer_ids.item_id,
        ic.topic_id,
        ic.cluster_id,
        IF(offer_type_label is not null, count_booking, null) as count_booking
    FROM `{{ bigquery_analytics_dataset }}`.enriched_offer_metadata offer
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.offer_item_ids offer_ids on offer.offer_id=offer_ids.offer_id
    LEFT JOIN item_clusters ic on ic.item_id = offer_ids.item_id
    LEFT JOIN offer_booking_information_view obi on obi.offer_id = offer.offer_id
)

SELECT * except(count_booking, offer_id)
FROM enriched_items
WHERE item_id is not null
QUALIFY ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY count_booking DESC) = 1
