select
    venue_id,
    venue_name,
    venue_description,
    venue_public_name,
    venue_siret,
    venue_is_virtual,
    offerer_id,
    offerer_validation_status,
    offerer_is_active,
    venue_creation_date,
    venue_type_label
from {{ ref("int_global__venue") }}
