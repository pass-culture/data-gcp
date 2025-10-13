select
    venue_id,
    venue_name,
    venue_description,
    venue_public_name,
    venue_siret,
    venue_is_virtual,
    venue_managing_offerer_id as offerer_id,
    venue_creation_date,
    venue_type_code as venue_type_label
from {{ ref("int_raw__venue_removed") }}
