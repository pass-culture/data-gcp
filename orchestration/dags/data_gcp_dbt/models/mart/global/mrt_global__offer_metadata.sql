{{
    config(
        cluster_by="offer_id",
    )
}}

select
    offer_id,
    offer_creation_date,
    offer_subcategory_id,
    offer_category_id,
    search_group_name,
    offer_type_domain,
    offer_name,
    offer_description,
    image_url,
    offer_type_id,
    offer_sub_type_id,
    author,
    performer,
    titelive_gtl_id,
    gtl_type,
    gtl_label_level_1,
    gtl_label_level_2,
    gtl_label_level_3,
    gtl_label_level_4,
    offer_type_label,
    offer_sub_type_label,
    offer_video_url,
    -- Sort the array elements alphabetically to ensure the hash remains the same
    -- regardless of the order they were originally inserted.
    array(
        select label from unnest(offer_type_labels) as label order by label
    ) as offer_type_labels
from {{ ref("int_applicative__offer_metadata") }}
