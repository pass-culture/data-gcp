with
    enriched_items as (
        select
            offer.offer_id,
            offer.item_id,
            offer.offer_creation_date,
            offer.offer_subcategory_id,
            offer.offer_category_id,
            offer.offer_name,
            offer.offer_description,
            offer.offer_type_domain,
            offer.author,
            offer.performer,
            offer.titelive_gtl_id,
            offer_metadata.search_group_name,
            offer_metadata.image_url,
            offer_metadata.offer_type_id,
            offer_metadata.offer_sub_type_id,
            offer_metadata.gtl_type,
            offer_metadata.gtl_label_level_1,
            offer_metadata.gtl_label_level_2,
            offer_metadata.gtl_label_level_3,
            offer_metadata.gtl_label_level_4,
            offer_metadata.offer_type_label,
            offer_metadata.offer_type_labels,
            offer_metadata.offer_sub_type_label,
            if(
                offer_metadata.offer_type_label is not null, offer.total_used_individual_bookings, null
            ) as total_used_individual_bookings
        from {{ ref("mrt_global__offer") }} as offer
        left join
            {{ ref("mrt_global__offer_metadata") }} as offer_metadata
            on offer.offer_id = offer_metadata.offer_id
    )

select * except (total_used_individual_bookings, offer_id)
from enriched_items
where item_id is not null
qualify
    row_number() over (
        partition by item_id order by total_used_individual_bookings desc
    )
    = 1
