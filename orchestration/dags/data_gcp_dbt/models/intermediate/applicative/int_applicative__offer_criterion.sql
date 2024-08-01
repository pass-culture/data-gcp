select
    adc.name as tag_name,
    adc.id as criterion_id,
    adoc.offerid as offer_id,
    ado.offer_name as offer_name
from {{ source("raw", "applicative_database_criterion") }} adc
    left join {{ source("raw", "applicative_database_offer_criterion") }} adoc on adoc.criterionid = adc.id
    left join {{ source("raw", "applicative_database_offer") }} ado on ado.offer_id = adoc.offerid
