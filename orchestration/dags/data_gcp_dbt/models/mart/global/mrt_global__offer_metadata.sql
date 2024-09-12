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
    offer_type_labels,
    offer_sub_type_label
from {{ ref('int_applicative__offer_metadata') }}