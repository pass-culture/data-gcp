select edv.educational_domain_id, edv.venue_id, ed.educational_domain_name
from {{ source("raw_eu1", "applicative_database_educational_domain_venue") }} edv
left join
    {{ source("raw_eu1", "applicative_database_educational_domain") }} as ed
    on edv.educational_domain_id = ed.educational_domain_id
