SELECT 
    adc.name as tag_name,
    adc.id as criterion_id,
    adoc.offerId as offer_id,
    ado.offer_name as offer_name
FROM {{ source("raw", "applicative_database_criterion") }} adc 
LEFT JOIN {{ source("raw", "applicative_database_offer_criterion") }} adoc on adoc.criterionId = adc.id
LEFT JOIN {{ source("raw", "applicative_database_offer") }}  ado on ado.offer_id = adoc.offerId