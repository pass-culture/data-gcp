with enriched_items as (
    select
        o.offer_id,
        o.item_id,
        IF(offer_type_label is not null, o.total_used_individual_bookings, null) as total_used_individual_bookings,
        o.offer_creation_date,
        o.offer_subcategory_id,
        o.offer_category_id,
        o.offer_name,
        o.offer_description,
        o.offer_type_domain,
        o.author,
        o.performer,
        o.titelive_gtl_id,
        search_group_name,
        image_url,
        offer_type_id,
        offer_sub_type_id,
        gtl_type,
        gtl_label_level_1,
        gtl_label_level_2,
        gtl_label_level_3,
        gtl_label_level_4,
        offer_type_label,
        offer_type_labels,
        offer_sub_type_label,
    from {{ ref('int_global__offer') }} AS o
        left join {{ ref('int_applicative__offer_metadata') }} as om ON o.offer_id = om.offer_id
)

select * except (total_used_individual_bookings, offer_id)
from enriched_items
where item_id is not null
qualify ROW_NUMBER() over (partition by item_id order by total_used_individual_bookings desc) = 1
