{% set target_name = target.name %}

{{ config(
    pre_hook="{{create_humanize_id_function()}}"
) }}

SELECT offerer_is_active,
    offerer_address,
    offerer_postal_code,
    offerer_city,
    offerer_id,
    offerer_creation_date,
    offerer_name,
    offerer_siren,
    offerer_validation_status,
    offerer_validation_date,
    "" AS offerer_humanized_id
FROM {{ source("raw", "applicative_database_offerer") }}
