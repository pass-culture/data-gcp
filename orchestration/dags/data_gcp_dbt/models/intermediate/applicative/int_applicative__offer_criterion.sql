select
    adc.name as tag_name,
    adc.id as criterion_id,
    adc.description,
    adc.highlight_id,
    adcc.criterion_category_label,
    adoc.offerid as offer_id,
    ado.offer_name,
    date(adc.startdatetime) as criterion_beginning_date,
    date(adc.enddatetime) as criterion_ending_date
from {{ source("raw", "applicative_database_criterion") }} as adc
left join
    {{ source("raw", "applicative_database_offer_criterion") }} as adoc
    on adc.id = adoc.criterionid
left join {{ ref("int_raw__offer") }} as ado on adoc.offerid = ado.offer_id
left join
    {{ source("raw", "applicative_database_criterion_category_mapping") }} as adccm
    on adc.id = adccm.criterion_id
left join
    {{ source("raw", "applicative_database_criterion_category") }} as adcc
    on adccm.criterion_category_id = adcc.criterion_category_id
