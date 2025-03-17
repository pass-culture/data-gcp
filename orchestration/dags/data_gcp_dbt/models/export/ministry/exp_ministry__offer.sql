{{
    config(
        tags="monthly",
        labels={"schedule": "monthly"}
    )
}}

select
    offer_id,
    offer_name,
    offer_category_id,
    offer_created_at,
    offer_creation_date,
    offer_product_id,
    is_synchronised,
    offer_description,
    offer_updated_date,
    offer_is_duo,
    item_id,
    offer_is_bookable,
    digital_goods,
    physical_goods,
    event,
    last_stock_price,
    webapp_url,
    offer_subcategory_id,
    offer_url,
    is_national,
    is_active,
    offer_validation,
    release_date,
    countries,
    venue_id,
    offerer_id
from {{ ref("mrt_global__offer") }}
