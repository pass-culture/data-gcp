WITH base as(
SELECT 
        offer_item_id.item_id                                           AS item_id,
        subcategories.category_id                                       AS offer_categoryId,
        offer.offer_subcategoryId                                       AS offer_subcategoryid,
        item_embedding_reduced.image_embedding                          AS item_image_embedding,
        item_embedding_reduced.semantic_content_embedding               AS item_semantic_content_hybrid_embedding,
        STRING_AGG(DISTINCT enroffer.offer_name, " ")                   AS item_names,
        STRING_AGG(DISTINCT offer.offer_description, " ")               AS item_descriptions,
        STRING_AGG(DISTINCT enroffer.rayon, " ")                        AS item_rayons,
        STRING_AGG(DISTINCT enroffer.author, " ")                       AS item_author,
        STRING_AGG(DISTINCT enroffer.performer, " ")                    AS item_performer,
        ROUND(AVG(enroffer.last_stock_price), -1)                       AS item_mean_stock_price,
        ROUND(SUM(enroffer.total_used_individual_bookings), -1)                    AS item_booking_cnt,
        ROUND(SUM(enroffer.total_favorites), -1)                          AS item_favourite_cnt,


FROM `{{ bigquery_analytics_dataset }}`.global_offer enroffer
INNER JOIN `{{ bigquery_clean_dataset }}`.`applicative_database_offer` offer
        ON enroffer.offer_id = offer.offer_id
INNER JOIN `{{ bigquery_analytics_dataset }}`.`subcategories` subcategories
        ON offer.offer_subcategoryId = subcategories.id
INNER JOIN `{{ bigquery_int_applicative_dataset }}`.`offer_item_id` offer_item_id
        ON offer_item_id.offer_id = offer.offer_id
INNER JOIN `{{ bigquery_ml_preproc_dataset }}`.`item_embedding_reduced_16` item_embedding_reduced
        ON offer_item_id.item_id = item_embedding_reduced.item_id
GROUP BY 1,2,3,4,5
)

SELECT * 
FROM base
QUALIFY ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY item_booking_cnt DESC) = 1