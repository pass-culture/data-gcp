with
    source as (
        select
            partition_month,
            region_name,
            region_code,
            department_name,
            department_code,
            age_at_calculation,
            is_in_qpv,
            macro_density_label,
            micro_density_label,
            sum(total_actual_beneficiaries) as total_actual_beneficiaries,
            sum(total_beneficiaries) as total_beneficiaries,
            mod(abs(sum(cell_key_beneficiaries)), 256) as cell_key_beneficiaries,
            mod(
                abs(sum(cell_key_actual_beneficiaries)), 256
            ) as cell_key_actual_beneficiaries
        from {{ ref("metrics_beneficiary") }}
        group by
            partition_month,
            region_name,
            region_code,
            department_name,
            department_code,
            age_at_calculation,
            is_in_qpv,
            macro_density_label,
            micro_density_label
    )

select
    s.partition_month,
    s.region_name,
    s.region_code,
    s.department_name,
    s.department_code,
    s.age_at_calculation,
    s.is_in_qpv,
    s.macro_density_label,
    s.micro_density_label,
    {{
        apply_perturbation(
            "s.total_actual_beneficiaries", "total_actual_beneficiaries", "pt_actual"
        )
    }},
    {{ apply_perturbation("s.total_beneficiaries", "total_beneficiaries", "pt_benef") }}
from
    source as s
    {{
        perturbation_join(
            "pt_benef", "s.total_beneficiaries", "s.cell_key_beneficiaries"
        )
    }}
    {{
        perturbation_join(
            "pt_actual",
            "s.total_actual_beneficiaries",
            "s.cell_key_actual_beneficiaries",
        )
    }}
