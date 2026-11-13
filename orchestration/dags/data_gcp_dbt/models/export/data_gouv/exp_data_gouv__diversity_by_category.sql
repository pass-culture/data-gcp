-- data.gouv.fr diversity by category, from the perturbed base, closed months
-- only. The denominator is total_expired_deposit_beneficiaries of
-- exp_data_gouv__diversity (same month and cell)
select
    deposit_expiration_month,
    {{ data_gouv_geo_columns() }},
    is_in_qpv,
    macro_density_label,
    micro_density_label,
    offer_category_id,
    total_category_booked_beneficiaries
from {{ ref("exp_base__diversity_by_category") }}
-- from May 2023: first expirations of the credits granted since the national
-- opening in May 2021 (24-month credits); earlier ones are pilot cohorts
-- a row whose counts are all 0 is a cell of 1 to 5 young people perturbed to 0:
-- left out, so that a missing combination reads as 0 or a very small count
where
    {{ data_gouv_month_filter("deposit_expiration_month", "2023-05-01") }}
    and total_category_booked_beneficiaries > 0
