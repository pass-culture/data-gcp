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
            offer_category_id,
            sum(
                total_category_booked_beneficiaries
            ) as total_category_booked_beneficiaries,
            mod(abs(sum(cell_key_category)), 256) as cell_key_category
        from {{ ref("metrics_diversity__by_category") }}
        group by
            deposit_expiration_month,
            region_name,
            region_code,
            department_name,
            department_code,
            is_in_qpv,
            macro_density_label,
            micro_density_label,
            offer_category_id
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
    s.offer_category_id,
    greatest(
        s.total_category_booked_beneficiaries + pt.perturbation, 0
    ) as total_category_booked_beneficiaries
from source as s
inner join
    {{ ref("perturbation_table__individual") }} as pt
    on s.total_category_booked_beneficiaries between pt.count_min and pt.count_max
    and s.cell_key_category between pt.cell_key_min and pt.cell_key_max
where s.deposit_expiration_month > "2021-01-01"
