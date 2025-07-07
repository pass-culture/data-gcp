select
    offerer_id,
    offerer_name,
    offerer_creation_date,
    offerer_validation_date,
    offerer_siren,
    legal_unit_business_activity_code,
    legal_unit_business_activity_label,
    legal_unit_legal_category_code,
    legal_unit_legal_category_label
from {{ ref("mrt_global__offerer") }}
