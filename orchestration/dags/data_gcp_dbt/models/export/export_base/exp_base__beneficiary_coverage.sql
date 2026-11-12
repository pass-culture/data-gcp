with source as (select * from {{ ref("metrics_beneficiary__coverage") }})

select
    s.partition_month,
    s.region_name,
    s.region_code,
    s.department_name,
    s.department_code,
    s.milestone_age,
    {{
        apply_perturbation(
            "s.total_beneficiaries_last_12_months",
            "total_beneficiaries_last_12_months",
            "pt",
        )
    }}
from
    source as s
    {{
        perturbation_join(
            "pt", "s.total_beneficiaries_last_12_months", "s.cell_key_beneficiaries"
        )
    }}
-- coverage 15+ to 18+ (milestone 16 to 19), the ages of the vidoc sheet and
-- external_reporting; 19+ would only add late activations
where s.milestone_age between 16 and 19
