select
    venue_id,
    venue_name,
    venue_description,
    venue_public_name,
    venue_siret,
    venue_is_virtual,
    venue_managing_offerer_id as offerer_id,
    venue_creation_date,
    venue_type_code as venue_type_label,
    venue_department_code,
    venue_city,
    venue_postal_code,
    venue_latitude,
    venue_longitude
from {{ ref("int_raw__venue_removed") }}
