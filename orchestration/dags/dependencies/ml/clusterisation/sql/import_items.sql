WITH booking_info AS(
    SELECT
        o.item_id,
        o.offer_subcategoryid,
        IFNULL(SUM(booking_quantity), 0) as booking_cnt
    FROM
        `{{ bigquery_analytics_dataset }}.enriched_offer_data` o
        LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_booking_data` b ON o.offer_id = b.offer_id
        AND booking_creation_date >= DATE_SUB(current_date, INTERVAL 6 MONTH)
    GROUP BY
        1,
        2
    ORDER BY
        2 DESC
),
item_top_N_booking_by_cat as(
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY offer_subcategoryid
            ORDER BY
                booking_cnt DESC
        ) AS rank
    FROM
        booking_info 
    where booking_cnt>0

),
items_w_embedding as (
    SELECT
        ie.item_id,
        ie.offer_semantic_content_optim_text,
    FROM
        `{{ bigquery_clean_dataset }}`.item_embeddings_semantic_content_reduced_5 ie
    ORDER BY
        ie.extraction_date DESC
),
base as (
    select
        top_items.item_id,
        top_items.rank,
        ie.offer_semantic_content_optim_text,
        enriched_item_metadata.subcategory_id AS subcategory_id,
        enriched_item_metadata.category_id as category,
        enriched_item_metadata.offer_type_id,
        enriched_item_metadata.offer_type_label,
        enriched_item_metadata.offer_sub_type_id,
        enriched_item_metadata.offer_sub_type_label,
    from
        item_top_N_booking_by_cat top_items
        JOIN items_w_embedding ie on ie.item_id = top_items.item_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.enriched_item_metadata enriched_item_metadata on top_items.item_id = enriched_item_metadata.item_id
)
select
    *
from
    base