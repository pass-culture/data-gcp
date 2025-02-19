select
    p.provider_id,
    p.provider_name,
    p.provider_is_active,
    p.venue_provider_is_active,
    p.venue_id,
    p.last_sync_date,
    p.creation_date,
    v.venue_name,
    v.venue_department_code,
    v.venue_department_name,
    v.venue_creation_date,
    v.venue_is_permanent,
    v.venue_label,
    p.booking_external_url,
    p.cancel_external_url,
    p.notification_external_url,
    count(distinct o.offer_id) as total_individual_offers,
    count(distinct co.collective_offer_id) as total_collective_offers,
    min(offer_creation_date) as first_individual_offer_creation_date,
    min(collective_offer_creation_date) as first_collective_offer_creation_date
from {{ ref("int_applicative__venue_provider") }} as p
left join {{ ref("int_global__venue") }} as v on p.venue_id = v.venue_id
left join
    {{ ref("int_applicative__offer") }} as o
    on o.venue_id = v.venue_id
    and o.offer_last_provider_id = p.provider_id
left join
    {{ ref("int_applicative__collective_offer") }} as co
    on co.provider_id = p.provider_id
    and co.venue_id = v.venue_id
group by
    provider_id,
    provider_name,
    provider_is_active,
    venue_provider_is_active,
    venue_id,
    venue_name,
    venue_department_code,
    venue_department_name,
    venue_creation_date,
    venue_is_permanent,
    venue_label,
    last_sync_date,
    creation_date,
    booking_external_url,
    cancel_external_url,
    notification_external_url
