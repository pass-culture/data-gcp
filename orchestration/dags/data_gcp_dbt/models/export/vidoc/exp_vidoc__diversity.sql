with
    source as (
        select
            deposit_expiration_month,
            region_name,
            region_code,
            department_name,
            department_code,
            is_in_qpv,
            macro_density_label,
            micro_density_label,
            sum(
                total_3plus_category_booked_beneficiaries
            ) as total_3plus_category_booked_beneficiaries,
            sum(
                total_expired_credit_beneficiaries
            ) as total_expired_credit_beneficiaries,
            mod(abs(sum(cell_key_3plus)), 256) as cell_key_3plus,
            mod(abs(sum(cell_key_expired)), 256) as cell_key_expired
        from {{ ref("metrics_diversity") }}
        group by
            deposit_expiration_month,
            region_name,
            region_code,
            department_name,
            department_code,
            is_in_qpv,
            macro_density_label,
            micro_density_label
    )

select
    s.deposit_expiration_month,
    s.region_name,
    s.region_code,
    s.department_name,
    s.department_code,
    s.is_in_qpv,
    s.macro_density_label,
    s.micro_density_label,
    {{
        apply_perturbation(
            "s.total_3plus_category_booked_beneficiaries",
            "total_3plus_category_booked_beneficiaries",
            "pt_3plus",
        )
    }},
    {{
        apply_perturbation(
            "s.total_expired_credit_beneficiaries",
            "total_expired_credit_beneficiaries",
            "pt_expired",
        )
    }}
from
    source as s
    {{
        perturbation_join(
            "pt_3plus",
            "s.total_3plus_category_booked_beneficiaries",
            "s.cell_key_3plus",
        )
    }}
    {{
        perturbation_join(
            "pt_expired", "s.total_expired_credit_beneficiaries", "s.cell_key_expired"
        )
    }}
where s.deposit_expiration_month > "2021-01-01"
