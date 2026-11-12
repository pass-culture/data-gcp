-- vidoc export of the perturbed base, see exp_base__diversity_by_category
select
    deposit_expiration_month,
    region_name,
    region_code,
    department_name,
    department_code,
    is_in_qpv,
    macro_density_label,
    micro_density_label,
    offer_category_id,
    total_category_booked_beneficiaries
from {{ ref("exp_base__diversity_by_category") }}
