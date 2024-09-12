{{ config(**custom_table_config()) }}

with items_grouping as (
    select
        offer.offer_id,
        case
            when (linked_offers.item_linked_id is not null and offer_product_id is null) then linked_offers.item_linked_id
            when (offer.offer_product_id is not null) then CONCAT('product-', offer.offer_product_id)
            else CONCAT('offer-', offer.offer_id)
        end as item_id
    from {{ source('raw','applicative_database_offer') }} as offer
        left join {{ source('analytics','linked_offers') }} linked_offers on linked_offers.offer_id = offer.offer_id
    qualify ROW_NUMBER() over (partition by offer_id order by offer_date_updated desc) = 1
)

select
    offer_id,
    MAX(item_id) as item_id
from
    items_grouping
where
    offer_id is not null
    and item_id is not null
group by
    offer_id
