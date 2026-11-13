-- data.gouv.fr beneficiary coverage, from the perturbed base (numerator of the
-- coverage rate). The INSEE population denominator is not republished: it is
-- public INSEE data, see the documentation for the computation code
select
    partition_month,
    {{ data_gouv_geo_columns() }},
    milestone_age,
    total_beneficiaries_last_12_months
from {{ ref("exp_base__beneficiary_coverage") }}
-- from May 2021: national opening of the individual credit at 18 (pilot before)
-- a row whose counts are all 0 is a cell of 1 to 5 young people perturbed to 0:
-- left out, so that a missing combination reads as 0 or a very small count
where
    {{ data_gouv_month_filter("partition_month", "2021-05-01") }}
    and total_beneficiaries_last_12_months > 0
