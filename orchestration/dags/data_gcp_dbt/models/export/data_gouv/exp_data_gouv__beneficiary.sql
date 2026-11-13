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
where {{ data_gouv_month_filter("partition_month", "2021-05-01") }}
