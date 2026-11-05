select cod.collective_offer_id, cod.educational_domain_id, ed.educational_domain_name
from {{ source("raw", "applicative_database_collective_offer_domain") }} as cod
left join
    {{ source("raw", "applicative_database_educational_domain") }} as ed
    on cod.educational_domain_id = ed.educational_domain_id
union all
select
    -- same "template-" prefix as int_applicative__collective_offer: template ids
    -- share the id space of offer ids, so a bare id collides with a real offer
    concat("template-", cotd.collective_offer_template_id) as collective_offer_id,
    cotd.educational_domain_id,
    ed.educational_domain_name
from
    {{ source("raw", "applicative_database_collective_offer_template_domain") }} as cotd
left join
    {{ source("raw", "applicative_database_educational_domain") }} as ed
    on cotd.educational_domain_id = ed.educational_domain_id
