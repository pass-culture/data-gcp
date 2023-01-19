SELECT offer_item_ids.item_id                          AS item_id,
       subcategories.category_id                       AS offer_categoryId,
       offer.offer_subcategoryId                       AS offer_subcategoryid,
       STRING_AGG(DISTINCT enroffer.offer_name, " ") AS item_names,
       STRING_AGG(DISTINCT enroffer.rayon, " ")      AS item_rayons,
       STRING_AGG(DISTINCT enroffer.author, " ")       AS item_author,
       STRING_AGG(DISTINCT enroffer.performer, " ")    AS item_performer,
       ROUND(AVG(enroffer.last_stock_price), -1)       AS item_mean_stock_price,
       ROUND(SUM(enroffer.booking_confirm_cnt), -1)    AS item_booking_cnt,
       ROUND(SUM(enroffer.favourite_cnt), -1)          AS item_favourite_cnt

FROM `{{ bigquery_analytics_dataset }}`.enriched_offer_data enroffer
         INNER JOIN `{{ bigquery_clean_dataset }}`.`applicative_database_offer` offer
                    ON enroffer.offer_id = offer.offer_id
         INNER JOIN `{{ bigquery_clean_dataset }}`.`subcategories` subcategories
                    ON offer.offer_subcategoryId = subcategories.id
         INNER JOIN `{{ bigquery_analytics_dataset }}`.`offer_item_ids` offer_item_ids
                    ON offer_item_ids.offer_id = offer.offer_id

GROUP BY offer_item_ids.item_id,
         subcategories.category_id,
         offer.offer_subcategoryId