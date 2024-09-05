select
    cod.collective_offer_id,
    cod.educational_domain_id,
    ed.educational_domain_name
from {{ source('raw', 'applicative_database_collective_offer_domain') }} as cod
    left join {{ source('raw', 'applicative_database_educational_domain') }} as ed on cod.educational_domain_id = ed.educational_domain_id
