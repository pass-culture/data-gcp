with
    base as (
        select
            offer_item_id.item_id as item_id,
            subcategories.category_id as offer_category_id,
            offer.offer_subcategory_id as offer_subcategory_id,
            item_embedding_reduced.image_embedding as item_image_embedding,
            item_embedding_reduced.semantic_content_embedding
            as item_semantic_content_hybrid_embedding,
            string_agg(distinct enroffer.offer_name, " ") as item_names,
            string_agg(distinct offer.offer_description, " ") as item_descriptions,
            string_agg(distinct enroffer.rayon, " ") as item_rayons,
            string_agg(distinct enroffer.author, " ") as item_author,
            string_agg(distinct enroffer.performer, " ") as item_performer,
            round(avg(enroffer.last_stock_price), -1) as item_mean_stock_price,
            round(sum(enroffer.total_used_individual_bookings), -1) as item_booking_cnt,
            round(sum(enroffer.total_favorites), -1) as item_favourite_cnt,

        from `{{ bigquery_analytics_dataset }}`.global_offer enroffer
        inner join
            `{{ bigquery_clean_dataset }}`.`applicative_database_offer` offer
            on enroffer.offer_id = offer.offer_id
        inner join
            `{{ bigquery_raw_dataset }}`.`subcategories` subcategories
            on offer.offer_subcategory_id = subcategories.id
        inner join
            `{{ bigquery_int_applicative_dataset }}`.`offer_item_id` offer_item_id
            on offer_item_id.offer_id = offer.offer_id
        inner join
            `{{ bigquery_ml_preproc_dataset }}`.`item_embedding_reduced_16` item_embedding_reduced
            on offer_item_id.item_id = item_embedding_reduced.item_id
        group by 1, 2, 3, 4, 5
    )

select *
from base
qualify row_number() over (partition by item_id order by item_booking_cnt desc) = 1
