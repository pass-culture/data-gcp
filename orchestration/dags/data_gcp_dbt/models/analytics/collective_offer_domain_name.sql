select
    cod.collective_offer_id,
    cod.educational_domain_id,
    ed.educational_domain_name
from {{ source('raw', 'applicative_database_collective_offer_domain') }} cod
    left join {{ source('raw', 'applicative_database_educational_domain') }} ed on cod.educational_domain_id = ed.educational_domain_id
