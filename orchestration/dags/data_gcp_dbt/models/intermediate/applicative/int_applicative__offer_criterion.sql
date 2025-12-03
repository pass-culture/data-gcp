select
    adc.name as tag_name,
    adc.id as criterion_id,
    adc.description,
    adc.highlight_id,
    adcc.criterion_category_label,
    date(adc.startdatetime) as criterion_beginning_date,
    date(adc.enddatetime) as criterion_ending_date,
    adoc.offerid as offer_id,
    ado.offer_name as offer_name
from {{ source("raw", "applicative_database_criterion") }} adc
left join
    {{ source("raw", "applicative_database_offer_criterion") }} adoc
    on adoc.criterionid = adc.id
left join {{ ref("int_raw__offer") }} ado on ado.offer_id = adoc.offerid
left join
    {{ source("raw", "applicative_database_criterion_category_mapping") }} adccm
    on adccm.criterion_id = adc.id
left join
    {{ source("raw", "applicative_database_criterion_category") }} adcc
    on adcc.criterion_category_id = adccm.criterion_category_id
