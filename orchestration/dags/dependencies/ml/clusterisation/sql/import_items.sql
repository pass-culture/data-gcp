
with item_top_N_booking_by_cat as(
select distinct(oii.item_id),
ROW_NUMBER() OVER (
                PARTITION BY eim.category_id
                ORDER BY
                    booking_numbers.booking_number DESC
            ) as rnk
from `{{ bigquery_analytics_dataset }}`.offer_item_ids oii
LEFT JOIN `{{ bigquery_analytics_dataset }}`.enriched_item_metadata eim on eim.item_id=oii.item_id
LEFT JOIN (
        SELECT
            COUNT(*) AS booking_number,
            offer.item_id as item_id
        FROM
            `{{ bigquery_clean_dataset }}`.booking booking
            LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_stock stock ON booking.stock_id = stock.stock_id
            LEFT JOIN `{{ bigquery_analytics_dataset }}`.enriched_offer_data offer ON stock.offer_id = offer.offer_id
        WHERE
            booking.booking_creation_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
            AND NOT booking.booking_is_cancelled
        GROUP BY
            offer.item_id
    ) booking_numbers ON booking_numbers.item_id = oii.item_id
QUALIFY rnk <= 1000
),
items_w_embedding as (
SELECT
ie.item_id,
ie.offer_semantic_content_optim_text,
FROM`{{ bigquery_clean_dataset }}`.item_embeddings_semantic_content_reduced_5 ie
ORDER BY ie.extraction_date DESC
), 
base as (
select 
top_items.item_id,
ie.offer_semantic_content_optim_text,
enriched_item_metadata.subcategory_id AS subcategory_id,
enriched_item_metadata.category_id as category,
enriched_item_metadata.offer_type_id,
enriched_item_metadata.offer_type_label,
enriched_item_metadata.offer_sub_type_id,
enriched_item_metadata.offer_sub_type_label,
from item_top_N_booking_by_cat top_items
JOIN items_w_embedding ie on ie.item_id=top_items.item_id
LEFT JOIN `{{ bigquery_analytics_dataset }}`.enriched_item_metadata enriched_item_metadata on top_items.item_id=enriched_item_metadata.item_id
)
select * from base 
