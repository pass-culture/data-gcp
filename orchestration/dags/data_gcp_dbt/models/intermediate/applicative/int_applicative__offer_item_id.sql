{{ config(**custom_table_config()) }}

with
    items_grouping as (
        select
            offer.offer_id,
            case
                when
                    (
                        linked_offers.item_linked_id is not null
                        and offer.offer_product_id is null
                    )
                then linked_offers.item_linked_id
                when (offer.offer_product_id is not null)
                then concat('product-', offer.offer_product_id)
                else concat('offer-', offer.offer_id)
            end as item_id,
            case
                when (linkage_v2.item_id is not null and offer.offer_product_id is null)
                then linkage_v2.item_id
                when (offer.offer_product_id is not null)
                then concat('product-', offer.offer_product_id)
                else concat('offer-', offer.offer_id)
            end as new_item_id
        from {{ ref("int_raw__offer") }} as offer
        left join
            {{ source("analytics", "linked_offers") }} as linked_offers
            on offer.offer_id = linked_offers.offer_id
        left join
            {{ source("ml_linkage", "item_offer_mapping") }} as linkage_v2
            on offer.offer_id = linkage_v2.offer_id
        qualify
            row_number() over (
                partition by offer.offer_id order by offer.offer_updated_date desc
            )
            = 1
    )

select offer_id, max(item_id) as old_item_id, max(new_item_id) as item_id
from items_grouping
where offer_id is not null and item_id is not null
group by offer_id
