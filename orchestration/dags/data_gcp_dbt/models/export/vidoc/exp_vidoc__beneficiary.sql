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
    greatest(
        s.total_actual_beneficiaries + pt_actual.perturbation, 0
    ) as total_actual_beneficiaries,
    greatest(s.total_beneficiaries + pt_benef.perturbation, 0) as total_beneficiaries
from source as s
inner join
    {{ ref("perturbation_table__individual") }} as pt_benef
    on s.total_beneficiaries between pt_benef.count_min and pt_benef.count_max
    and s.cell_key_beneficiaries between pt_benef.cell_key_min and pt_benef.cell_key_max
inner join
    {{ ref("perturbation_table__individual") }} as pt_actual
    on s.total_actual_beneficiaries between pt_actual.count_min and pt_actual.count_max
    and s.cell_key_actual_beneficiaries
    between pt_actual.cell_key_min and pt_actual.cell_key_max
