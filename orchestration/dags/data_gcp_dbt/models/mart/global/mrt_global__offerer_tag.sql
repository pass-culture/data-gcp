select
    offerer_tag_mapping_id,
    offerer_id,
    tag_id,
    tag_name,
    tag_label,
    tag_description,
    tag_category_id,
    tag_category_name,
    tag_category_label
from {{ ref('int_applicative__offerer_tag') }}
