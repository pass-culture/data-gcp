select
    otm.offerer_tag_mapping_id,
    otm.offerer_id,
    otm.tag_id,
    ot.offerer_tag_name as tag_name,
    ot.offerer_tag_label as tag_label,
    ot.offerer_tag_description as tag_description,
    otcm.offerer_tag_category_id as tag_category_id,
    otc.offerer_tag_category_name as tag_category_name,
    otc.offerer_tag_category_label as tag_category_label
from {{ source("raw", "applicative_database_offerer_tag_mapping") }} as otm
inner join
    {{ source("raw", "applicative_database_offerer_tag") }} as ot
    on ot.offerer_tag_id = otm.tag_id
inner join
    {{ source("raw", "applicative_database_offerer_tag_category_mapping") }} as otcm
    on ot.offerer_tag_id = otcm.offerer_tag_id
inner join
    {{ source("raw", "applicative_database_offerer_tag_category") }} as otc
    on otcm.offerer_tag_category_id = otc.offerer_tag_category_id
