SELECT
    offer_id,
    offer_creation_date,
    offer_subcategory_id,
    offer_category_id,
    search_group_name,
    offer_type_domain,
    offer_name,
    offer_description,
    offer_type_id,
    gtl_label_level_1,
    gtl_label_level_2,
    gtl_label_level_3,
    gtl_label_level_4
FROM {{ref("mrt_global__offer_metadata")}}
