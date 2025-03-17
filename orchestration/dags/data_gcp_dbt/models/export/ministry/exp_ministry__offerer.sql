{{
    config(
        tags="monthly",
        labels={"schedule": "monthly"}
    )
}}

select
    offerer_id,
    partner_id,
    offerer_name,
    offerer_creation_date,
    offerer_validation_date,
    first_stock_creation_date,
    first_individual_offer_creation_date,
    last_individual_offer_creation_date,
    first_offer_creation_date,
    last_offer_creation_date,
    offerer_postal_code,
    offerer_department_code,
    offerer_siren,
    offerer_region_name,
    offerer_city,
    academy_name,
    legal_unit_business_activity_code,
    legal_unit_business_activity_label,
    legal_unit_legal_category_code,
    legal_unit_legal_category_label
from {{ ref("mrt_global__offerer") }}
