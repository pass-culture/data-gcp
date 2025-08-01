with
    base as (
        select
            offer.item_id,
            offer.offer_category_id,
            offer.offer_subcategory_id,
            item_embedding_reduced.image_embedding as item_image_embedding,
            item_embedding_reduced.semantic_content_embedding
            as item_semantic_content_hybrid_embedding,
            string_agg(distinct offer.offer_name, " ") as item_names,
            string_agg(distinct offer.offer_description, " ") as item_descriptions,
            string_agg(distinct offer.rayon, " ") as item_rayons,
            string_agg(distinct offer.author, " ") as item_author,
            string_agg(distinct offer.performer, " ") as item_performer,
            round(avg(offer.last_stock_price), -1) as item_mean_stock_price,
            round(sum(offer.total_used_individual_bookings), -1) as item_booking_cnt,
            round(sum(offer.total_favorites), -1) as item_favourite_cnt
        from {{ ref("mrt_global__offer") }} as offer
        inner join
            {{ source("ml_preproc", "item_embedding_reduced_16") }}
            as item_embedding_reduced
            on offer.item_id = item_embedding_reduced.item_id
        group by 1, 2, 3, 4, 5
    )

select *
from base
where item_mean_stock_price is not null
qualify row_number() over (partition by item_id order by item_booking_cnt desc) = 1
