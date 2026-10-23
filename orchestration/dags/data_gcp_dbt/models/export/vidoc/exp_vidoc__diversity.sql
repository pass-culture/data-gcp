{% set min_category_counts = get_min_category_counts() %}
{% set spent_share_buckets = get_spent_share_buckets() %}
{% set nested_counts = [] %}
{% for n in min_category_counts %}
    {% do nested_counts.append(
        {
            "column": "total_" ~ n ~ "plus_category_booked_beneficiaries",
            "cell_key": "cell_key_" ~ n ~ "plus",
        }
    ) %}
{% endfor %}
{% for bucket in spent_share_buckets %}
    {% do nested_counts.append(
        {
            "column": "total_spent_share_" ~ bucket.name ~ "_beneficiaries",
            "cell_key": "cell_key_spent_share_" ~ bucket.name,
        }
    ) %}
{% endfor %}

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
                total_expired_deposit_beneficiaries
            ) as total_expired_deposit_beneficiaries,
            mod(abs(sum(cell_key_expired)), 256) as cell_key_expired,
            {% for c in nested_counts %}
                sum({{ c.column }}) as {{ c.column }},
                mod(abs(sum({{ c.cell_key }})), 256) as {{ c.cell_key }},
            {% endfor %}
            sum(
                total_actual_amount_spent_at_expiration
            ) as total_actual_amount_spent_at_expiration,
            sum(
                total_distinct_venue_booked_at_expiration
            ) as total_distinct_venue_booked_at_expiration
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
            s.total_actual_amount_spent_at_expiration,
            s.total_distinct_venue_booked_at_expiration,
            {% for c in nested_counts %}
                {{ apply_perturbation("s." ~ c.column, c.column, "pt_" ~ loop.index) }},
            {% endfor %}
            {{
                apply_perturbation(
                    "s.total_expired_deposit_beneficiaries",
                    "total_expired_deposit_beneficiaries",
                    "pt_expired",
                )
            }}
        from
            source as s
            {% for c in nested_counts %}
                {{
                    perturbation_join(
                        "pt_" ~ loop.index, "s." ~ c.column, "s." ~ c.cell_key
                    )
                }}
            {% endfor %}
            {{
                perturbation_join(
                    "pt_expired",
                    "s.total_expired_deposit_beneficiaries",
                    "s.cell_key_expired",
                )
            }}
        where s.deposit_expiration_month > "2021-01-01"
    )

-- Nested cohorts (N+ categories, spent share buckets ⊂ expired): perturbations are
-- independent, so cap each perturbed numerator at the perturbed denominator to keep
-- shares <= 100%. Sums are not perturbed: they are hidden when the perturbed count is
-- below 5
select
    deposit_expiration_month,
    region_name,
    region_code,
    department_name,
    department_code,
    is_in_qpv,
    macro_density_label,
    micro_density_label,
    total_expired_deposit_beneficiaries,
    {% for c in nested_counts %}
        least({{ c.column }}, total_expired_deposit_beneficiaries) as {{ c.column }},
    {% endfor %}
    if(
        total_expired_deposit_beneficiaries >= 5,
        total_actual_amount_spent_at_expiration,
        null
    ) as total_actual_amount_spent_at_expiration,
    if(
        total_expired_deposit_beneficiaries >= 5,
        total_distinct_venue_booked_at_expiration,
        null
    ) as total_distinct_venue_booked_at_expiration
from perturbed
