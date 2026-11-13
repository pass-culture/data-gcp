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
-- from May 2023: first expirations of the credits granted since the national
-- opening in May 2021 (24-month credits); earlier ones are pilot cohorts
-- a row whose counts are all 0 is a cell of 1 to 5 young people perturbed to 0:
-- left out, so that a missing combination reads as 0 or a very small count
where
    {{ data_gouv_month_filter("deposit_expiration_month", "2023-05-01") }}
    and total_expired_deposit_beneficiaries > 0
