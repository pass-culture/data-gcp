select
    edv.educational_domain_venue_id,
    edv.educational_domain_id,
    edv.venue_id,
    ed.educational_domain_name
from {{ source("raw", "applicative_database_educational_domain_venue") }} edv
left join
    {{ source("raw", "applicative_database_educational_domain") }} as ed
    on edv.educational_domain_id = ed.educational_domain_id
