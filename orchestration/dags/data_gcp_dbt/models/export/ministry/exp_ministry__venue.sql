select
    v.venue_id,
    v.venue_name,
    v.venue_description,
    v.venue_public_name,
    v.venue_siret,
    v.venue_is_virtual,
    v.venue_managing_offerer_id as offerer_id,
    o.offerer_validation_status,
    o.offerer_is_active,
    v.venue_creation_date,
    v.venue_type_code as venue_type_label
from {{ source("raw", "applicative_database_venue") }} as v
left join {{ ref("int_global__offerer") }} as o on v.venue_managing_offerer_id = o.offerer_id
