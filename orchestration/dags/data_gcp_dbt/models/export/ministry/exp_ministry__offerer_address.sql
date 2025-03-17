{{
    config(
        tags="monthly",
        labels={"schedule": "monthly"}
    )
}}

select
    offerer_address_id,
    offerer_address_label,
    address_id,
    offerer_id,
    address_street,
    address_postal_code,
    address_city,
    address_department_code
from {{ ref("mrt_global__offerer_address") }}
