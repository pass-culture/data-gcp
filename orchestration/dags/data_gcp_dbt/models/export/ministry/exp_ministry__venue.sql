{{ config(tags="monthly", labels={"schedule": "monthly"}) }}

select
    venue_id,
    venue_name,
    venue_public_name,
    venue_booking_email,
    venue_street,
    venue_latitude,
    venue_longitude,
    venue_department_code,
    venue_postal_code,
    venue_city,
    venue_siret,
    venue_is_virtual,
    venue_managing_offerer_id,
    venue_creation_date,
    venue_is_acessibility_synched,
    venue_type_label,
    venue_label,
    venue_density_label,
    venue_macro_density_label,
    venue_description,
    offerer_id,
    partner_id
from {{ ref("mrt_global__venue") }}
