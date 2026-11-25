{{ config(**custom_table_config(cluster_by=["offer_id"])) }}

with
    event_series_offer_link as (
        select
            cast(offer_id as string) as offer_id,
            cast(event_series_id as string) as event_series_id
        from {{ source("raw", "applicative_database_event_series_offer_link") }}
    ),

    items_grouping as (
        select
            offer.offer_id,
            case
                when (offer.offer_product_id is not null)
                then concat('product-', offer.offer_product_id)
                when (event_series_offer_link.event_series_id is not null)
                then concat('event-', event_series_offer_link.event_series_id)
                when (linkage_v2.item_id is not null)
                then linkage_v2.item_id
                else concat('offer-', offer.offer_id)
            end as item_id
        from {{ ref("int_raw__offer") }} as offer
        left join
            event_series_offer_link
            on cast(offer.offer_id as string) = event_series_offer_link.offer_id
        left join
            {{ source("ml_linkage", "item_offer_mapping") }} as linkage_v2
            on offer.offer_id = linkage_v2.offer_id
        qualify
            row_number() over (
                partition by offer.offer_id order by offer.offer_updated_date desc
            )
            = 1
    )

select offer_id, max(item_id) as item_id
from items_grouping
where offer_id is not null and item_id is not null
group by offer_id
