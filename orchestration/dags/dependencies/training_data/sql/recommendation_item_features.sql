WITH base as(
SELECT 
offer_item_ids.item_id                                      AS item_id,
subcategories.category_id                                   AS offer_categoryId,
offer.offer_subcategoryId                                   AS offer_subcategoryid,
STRING_AGG(DISTINCT enroffer.offer_name, " ")               AS item_names,
STRING_AGG(DISTINCT offer.offer_description, " ")           AS item_descriptions,
STRING_AGG(DISTINCT enroffer.rayon, " ")                    AS item_rayons,
STRING_AGG(DISTINCT enroffer.author, " ")                   AS item_author,
STRING_AGG(DISTINCT enroffer.performer, " ")                AS item_performer,
ROUND(AVG(enroffer.last_stock_price), -1)                   AS item_mean_stock_price,
ROUND(SUM(enroffer.booking_confirm_cnt), -1)                AS item_booking_cnt,
ROUND(SUM(enroffer.favourite_cnt), -1)                      AS item_favourite_cnt,
item_embeddings_reduced.semantic_content_embedding          AS semantic_content_embedding,
item_embeddings_reduced.image_embedding                     AS item_image_embedding,
item_embeddings_reduced.semantic_content_hybrid_embedding   AS item_semantic_content_hybrid_embedding,

FROM `{{ bigquery_analytics_dataset }}`.enriched_offer_data enroffer
INNER JOIN `{{ bigquery_clean_dataset }}`.`applicative_database_offer` offer
        ON enroffer.offer_id = offer.offer_id
INNER JOIN `{{ bigquery_analytics_dataset }}`.`subcategories` subcategories
        ON offer.offer_subcategoryId = subcategories.id
INNER JOIN `{{ bigquery_analytics_dataset }}`.`offer_item_ids` offer_item_ids
        ON offer_item_ids.offer_id = offer.offer_id
INNER JOIN `{{ bigquery_clean_dataset }}`.`item_embeddings_reduced_16` item_embeddings_reduced
        ON offer_item_ids.item_id = item_embeddings_reduced.item_id
GROUP BY offer_item_ids.item_id,
        subcategories.category_id,
        offer.offer_subcategoryId,
        semantic_content_embedding,
        item_image_embedding,
        item_semantic_content_hybrid_embedding
)
select *  from base
QUALIFY ROW_NUMBER() OVER (
PARTITION BY item_id
ORDER BY
item_booking_cnt DESC
) = 1