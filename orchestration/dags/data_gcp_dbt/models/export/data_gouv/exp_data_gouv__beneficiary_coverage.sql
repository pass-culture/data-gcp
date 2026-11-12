-- data.gouv.fr copy of the vidoc lot 1 beneficiary coverage export (numerator of the
-- coverage rate). The INSEE population denominator is not republished: it is
-- public INSEE data, see the documentation for the computation code
select
    partition_month,
    {{ data_gouv_geo_columns() }},
    milestone_age,
    total_beneficiaries_last_12_months
from {{ ref("exp_vidoc__beneficiary_coverage") }}
where {{ data_gouv_closed_month_filter("partition_month") }}
