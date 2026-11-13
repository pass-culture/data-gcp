-- data.gouv.fr beneficiaries, from the perturbed base: same perturbed counts,
-- closed months only, geographic codes formatted for data.gouv.fr
select
    partition_month,
    {{ data_gouv_geo_columns() }},
    age_at_calculation,
    is_in_qpv,
    macro_density_label,
    micro_density_label,
    total_beneficiaries,
    total_actual_beneficiaries
from {{ ref("exp_base__beneficiary") }}
-- from May 2021: national opening of the individual credit at 18 (pilot before)
-- a row whose counts are all 0 is a cell of 1 to 5 young people perturbed to 0:
-- left out, so that a missing combination reads as 0 or a very small count
where
    {{ data_gouv_month_filter("partition_month", "2021-05-01") }}
    and total_beneficiaries > 0
