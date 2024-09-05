select
    collective_offer_id,
    educational_domain_id,
    educational_domain_name
from {{ ref('int_applicative__collective_offer_domain') }}
