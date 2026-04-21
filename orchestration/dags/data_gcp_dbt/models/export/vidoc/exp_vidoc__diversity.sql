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
    greatest(
        s.total_3plus_category_booked_beneficiaries + pt_3plus.perturbation, 0
    ) as total_3plus_category_booked_beneficiaries,
    greatest(
        s.total_expired_credit_beneficiaries + pt_expired.perturbation, 0
    ) as total_expired_credit_beneficiaries
from source as s
inner join
    {{ ref("perturbation_table__individual") }} as pt_3plus
    on s.total_3plus_category_booked_beneficiaries
    between pt_3plus.count_min and pt_3plus.count_max
    and s.cell_key_3plus between pt_3plus.cell_key_min and pt_3plus.cell_key_max
inner join
    {{ ref("perturbation_table__individual") }} as pt_expired
    on s.total_expired_credit_beneficiaries
    between pt_expired.count_min and pt_expired.count_max
    and s.cell_key_expired between pt_expired.cell_key_min and pt_expired.cell_key_max
where s.deposit_expiration_month > "2021-01-01"
