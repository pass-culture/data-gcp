{{ config(tags="monthly", labels={"schedule": "monthly"}) }}

select
    offer_id,
    offer_creation_date,
    offer_subcategory_id,
    offer_category_id,
    offer_type_domain,
    offer_name,
    offer_description,
    gtl_label_level_1,
    gtl_label_level_2,
    gtl_label_level_3,
    gtl_label_level_4
from {{ ref("mrt_global__offer_metadata") }}
