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
    ),

    perturbed as (
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
            {{
                apply_perturbation(
                    "s.total_category_booked_beneficiaries",
                    "total_category_booked_beneficiaries",
                    "pt",
                )
            }}
        from
            source as s
            {{
                perturbation_join(
                    "pt",
                    "s.total_category_booked_beneficiaries",
                    "s.cell_key_category",
                )
            }}
        where s.deposit_expiration_month > "2021-01-01"
    )

-- Nested cohort (category bookers ⊂ expired): perturbations are independent, so cap
-- the perturbed numerator at the published denominator of the same cell to keep
-- shares <= 100% at any aggregation level
select
    p.deposit_expiration_month,
    p.region_name,
    p.region_code,
    p.department_name,
    p.department_code,
    p.is_in_qpv,
    p.macro_density_label,
    p.micro_density_label,
    p.offer_category_id,
    least(
        p.total_category_booked_beneficiaries,
        coalesce(
            d.total_expired_deposit_beneficiaries, p.total_category_booked_beneficiaries
        )
    ) as total_category_booked_beneficiaries
from perturbed as p
left join
    {{ ref("exp_vidoc__diversity") }} as d
    on p.deposit_expiration_month = d.deposit_expiration_month
    and p.department_code = d.department_code
    and p.is_in_qpv is not distinct from d.is_in_qpv
    and p.macro_density_label is not distinct from d.macro_density_label
    and p.micro_density_label is not distinct from d.micro_density_label
