SELECT
    oc.criterion_id,
    oc.tag_name,
    oc.description,
    oc.criterion_category_label,
    oc.criterion_beginning_date,
    oc.criterion_ending_date,
    oc.offer_id,
    oc.offer_name
from {{ ref("int_applicative__offer_criterion") }} as oc
