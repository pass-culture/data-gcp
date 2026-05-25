{{
    config(
        tags="weekly",
        labels={"schedule": "weekly"},
    )
}}

select
    offer.offer_id,
    offer.offer_name,
    offer.offer_description,
    offer.offer_subcategory_id,
    offer_metadata.image_url
from {{ ref("mrt_global__offer") }} as offer
left join {{ ref("mrt_global__offer_metadata") }} as offer_metadata using (offer_id)
where
    offer.offer_subcategory_id in (
        'CONCERT',
        'SPECTACLE_REPRESENTATION',
        'FESTIVAL_MUSIQUE',
        'FESTIVAL_SPECTACLE',
        'EVENEMENT_MUSIQUE'
    )
    and offer.offer_is_bookable
