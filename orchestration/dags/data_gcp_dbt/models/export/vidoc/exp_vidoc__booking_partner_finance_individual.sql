{#
    Individual booking finance KPIs by month of use and venue department, with
    the statistical secret of external reporting (apply_statistical_secret): a
    department is secret when it is secret (is_department_secret_stat) at the
    last complete month, for any rolling window (6, 48 months) and on the
    individual or the collective side; all its months are then masked. Months
    from 2022 to the month before the run date, as external_reporting__individual.
#}
{% set last_month = (
    "date_trunc(date_sub(date('" ~ ds() ~ "'), interval 1 month), month)"
) %}
{% set secret_models = [
    "statistical_secret__cultural_partner_individual",
    "statistical_secret__cultural_partner_collective",
] %}

with
    aggregated_department_finance as (
        select
            partition_month,
            venue_department_code,
            venue_department_name,
            venue_region_name,
            max(venue_region_code) as venue_region_code,
            sum(total_bookings) as total_bookings,
            sum(total_quantities) as total_quantities,
            sum(total_revenue_amount) as total_revenue_amount,
            sum(total_reimbursed_amount) as total_reimbursed_amount,
            sum(total_contribution_amount) as total_contribution_amount
        from {{ ref("metrics_booking__finance_individual") }}
        where partition_month between date('2022-01-01') and {{ last_month }}
        group by
            partition_month,
            venue_department_code,
            venue_department_name,
            venue_region_name
    ),

    secret_source as (
        {% for secret_model in secret_models %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                department_code,
                is_department_secret_stat,
                format(
                    '%s (%s, %d mois)',
                    department_secret_reason_label,
                    '{{ "collectif" if "collective" in secret_model else "individuel" }}',
                    rolling_window_months_count
                ) as secret_detail
            from {{ ref(secret_model) }}
            where
                partition_month = {{ last_month }}
                and rolling_window_months_count in (6, 48)
        {% endfor %}
    ),

    secret_departments as (
        select
            department_code,
            logical_or(is_department_secret_stat) as is_department_secret,
            string_agg(
                distinct if(is_department_secret_stat, secret_detail, null),
                ' ; '
                order by if(is_department_secret_stat, secret_detail, null)
            ) as department_secret_reason_label
        from secret_source
        where department_code is not null
        group by department_code
    )

select
    facts.partition_month,
    facts.venue_department_code,
    facts.venue_department_name,
    facts.venue_region_name,
    facts.venue_region_code,
    coalesce(sec.is_department_secret, false) as is_statistic_secret,
    sec.department_secret_reason_label,
    {% for column in [
        "total_bookings",
        "total_quantities",
        "total_revenue_amount",
        "total_reimbursed_amount",
        "total_contribution_amount",
    ] %}
        if(
            coalesce(sec.is_department_secret, false), null, facts.{{ column }}
        ) as {{ column }}
        {% if not loop.last %},{% endif %}
    {% endfor %}
from aggregated_department_finance as facts
left join secret_departments as sec on facts.venue_department_code = sec.department_code
