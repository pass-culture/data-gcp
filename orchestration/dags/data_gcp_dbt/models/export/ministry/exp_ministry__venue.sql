select
    venue_id,
    venue_name,
    venue_description,
    venue_public_name,
    venue_siret,
    venue_is_virtual,
    offerer_id,
    venue_creation_date,
    venue_type_label
from {{ ref("mrt_global__venue") }}
