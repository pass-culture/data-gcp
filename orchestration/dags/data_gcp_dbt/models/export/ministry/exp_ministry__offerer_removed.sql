select
    offerer_id,
    offerer_name,
    offerer_creation_date,
    offerer_validation_date,
    offerer_validation_status,
    offerer_is_active,
    offerer_siren
from {{ ref("int_raw__offerer_removed") }}
