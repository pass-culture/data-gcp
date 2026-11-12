-- data.gouv.fr diversity, from the perturbed base: only the 3+ categories
-- indicator and its denominator (no amounts), closed months only
select
    deposit_expiration_month,
    {{ data_gouv_geo_columns() }},
    is_in_qpv,
    macro_density_label,
    micro_density_label,
    total_expired_deposit_beneficiaries,
    total_3plus_category_booked_beneficiaries
from {{ ref("exp_base__diversity") }}
where {{ data_gouv_closed_month_filter("deposit_expiration_month") }}
