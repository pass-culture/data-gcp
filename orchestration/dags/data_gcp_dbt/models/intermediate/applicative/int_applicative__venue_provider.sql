select
    p.is_active as provider_is_active,
    vp.is_active as venue_provider_is_active,
    p.provider_id,
    p.provider_name,
    p.local_class,
    p.enabled_for_pro,
    vp.venue_id,
    vp.last_sync_date,
    vp.creation_date
from {{ source('raw','applicative_database_provider') }} as p
    inner join {{ source('raw','applicative_database_venue_provider') }} as vp on vp.provider_id = p.provider_id
