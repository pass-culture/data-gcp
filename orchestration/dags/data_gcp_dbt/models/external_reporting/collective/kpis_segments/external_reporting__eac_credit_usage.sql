-- noqa: disable=all
{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{#
    EAC credit usage by school geography, rolled up from
    metrics_institution__activity (credit of the scholar year) and
    metrics_booking__collective_institution (amounts since the start of the
    scholar year).
    - taux_d_utilisation_du_credit: amount engaged / credit, every month of the
      scholar year up to the last complete month;
    - montant_moyen_par_reservation(_meme_territoire): reimbursed amount /
      reimbursed bookings (by month of use), optionally restricted to bookings
      whose venue is in the same territory as the school; reported for the
      territories with a reimbursed booking in the scholar year, on the months
      with a reimbursed booking nationally.
#}
{% set same_territory_level = {
    "NAT": none,
    "REG": "region",
    "ACAD": "academy",
    "EPCI": "epci",
    "COM": "municipality",
    "DEP": "department",
} %}
{% set dimensions = get_dimensions("institution", "academic_extended") %}

with
    budget as (
        {% for dim in dimensions %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                partition_month,
                scholar_year,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                sum(total_deposit) as total_budget_deposit
            from {{ ref("metrics_institution__activity") }}
            where
                partition_month
                <= date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
            group by partition_month, scholar_year, dimension_name, dimension_value
            having sum(total_institutions) > 0
        {% endfor %}
    ),

    bookings as (
        {% for dim in dimensions %}
            {% if not loop.first %}
                union all
            {% endif %}
            {% set suffix = (
                "_same_" ~ same_territory_level[dim.name]
                if same_territory_level[dim.name]
                else ""
            ) %}
            select
                partition_month,
                scholar_year,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                sum(
                    cumulative_total_collective_amount_spent
                ) as cumulative_amount_engaged,
                sum(
                    cumulative_total_reimbursed_collective_amount
                ) as cumulative_reimbursed_amount,
                sum(
                    cumulative_total_reimbursed_collective_bookings
                ) as cumulative_reimbursed_bookings,
                sum(
                    cumulative_total_reimbursed_collective_amount{{ suffix }}
                ) as cumulative_reimbursed_amount_same_territory,
                sum(
                    cumulative_total_reimbursed_collective_bookings{{ suffix }}
                ) as cumulative_reimbursed_bookings_same_territory,
                sum(sum(total_reimbursed_collective_bookings)) over (
                    partition by scholar_year, partition_month
                ) as national_monthly_reimbursed_bookings,
                sum(sum(total_reimbursed_collective_bookings)) over (
                    partition by
                        scholar_year
                        {% if dim.name != "NAT" %}, {{ dim.value_expr }}{% endif %}
                ) as scholar_year_reimbursed_bookings
            from {{ ref("metrics_booking__collective_institution") }}
            group by partition_month, scholar_year, dimension_name, dimension_value
        {% endfor %}
    ),

    reported_bookings as (
        select *
        from bookings
        where
            national_monthly_reimbursed_bookings > 0
            and scholar_year_reimbursed_bookings > 0
    )

select
    partition_month,
    timestamp("{{ ts() }}") as updated_at,
    dimension_name,
    dimension_value,
    kpi_name,
    numerator,
    denominator,
    safe_divide(numerator, denominator) as kpi
from
    (
        select
            partition_month,
            dimension_name,
            dimension_value,
            if(
                budget.total_budget_deposit is null,
                null,
                cast(coalesce(bookings.cumulative_amount_engaged, 0) as numeric)
            ) as taux_d_utilisation_du_credit_numerator,
            cast(
                budget.total_budget_deposit as numeric
            ) as taux_d_utilisation_du_credit_denominator,
            cast(
                reported.cumulative_reimbursed_amount as numeric
            ) as montant_moyen_par_reservation_numerator,
            cast(
                reported.cumulative_reimbursed_bookings as numeric
            ) as montant_moyen_par_reservation_denominator,
            cast(
                reported.cumulative_reimbursed_amount_same_territory as numeric
            ) as montant_moyen_par_reservation_meme_territoire_numerator,
            cast(
                reported.cumulative_reimbursed_bookings_same_territory as numeric
            ) as montant_moyen_par_reservation_meme_territoire_denominator
        from budget
        left join
            bookings using (
                partition_month, scholar_year, dimension_name, dimension_value
            )
        full outer join
            reported_bookings as reported using (
                partition_month, scholar_year, dimension_name, dimension_value
            )
        where
            dimension_value is not null
            {% if is_incremental() %}
                and partition_month
                = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
            {% endif %}
    ) unpivot (
        (numerator, denominator) for kpi_name in (
            (
                taux_d_utilisation_du_credit_numerator,
                taux_d_utilisation_du_credit_denominator
            ) as 'taux_d_utilisation_du_credit',
            (
                montant_moyen_par_reservation_numerator,
                montant_moyen_par_reservation_denominator
            ) as 'montant_moyen_par_reservation',
            (
                montant_moyen_par_reservation_meme_territoire_numerator,
                montant_moyen_par_reservation_meme_territoire_denominator
            ) as 'montant_moyen_par_reservation_meme_territoire'
        )
    )
