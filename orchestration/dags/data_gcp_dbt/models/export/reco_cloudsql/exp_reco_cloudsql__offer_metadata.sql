{{ config(materialized="view") }}

select offer_metadata.offer_id, offer_metadata.search_group_name
from {{ ref("mrt_global__offer_metadata") }} as offer_metadata
inner join
    {{ ref("int_applicative__offer") }} as offer
    on offer_metadata.offer_id = offer.offer_id
where
    -- Keep only offers displayed by the backend (GET native/v3/offer/<id>),
    -- mirroring pcapi Offer.isPublished.
    offer.offer_validation = 'APPROVED'
    and offer.offer_publication_date is not null
    and offer.offer_publication_date <= current_date + interval 1 day
