with
    base_offers as (
        select
            offer.offer_id,
            offer.item_id,
            offer.offer_subcategory_id,
            venue.venue_type_label,
            venue.offerer_name
        from {{ ref("mrt_global__offer") }} offer
        inner join {{ ref("int_raw__offer") }} using (offer_id)  -- Pourquoi ce Join @lrainteau-pass?
        inner join
            {{ ref("mrt_global__venue") }} venue
            on venue.venue_managing_offerer_id = offer.offerer_id
        where
            date(offer.offer_creation_date) >= date_sub(current_date, interval 6 month)
            and offer.offer_validation = "APPROVED"
            and offer.offer_product_id is null
            and offer.item_id not like 'isbn-%'
        group by 1, 2, 3, 4, 5
    )
select
    base_offers.*,
    ie.name_embedding as name_embedding,
    ie.description_embedding as description_embedding
from base_offers
join
    (
        select
            item_id,
            name_embedding as name_embedding,
            description_embedding as description_embedding
        from {{ source("ml_preproc", "item_embedding_extraction") }}
        where date(extraction_date) >= date_sub(current_date, interval 6 month)
        qualify
            row_number() over (partition by item_id order by extraction_date desc) = 1
    ) ie using (item_id)
