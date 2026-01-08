select
    offer_id,
    offer_name,
    offer_description,
    offer_subcategoryid as offer_subcategory,
    offer_creation_date as offer_created_at,
    offer_updated_date as offer_updated_at,
    offer_is_duo,
    offer_url,
    offer_is_national,
    offer_is_active,
    offerer_address_id,
    offer_publication_date,
    offer_product_id,
    venue_id,
    offer_validation
from {{ ref("int_raw__offer_removed") }}
