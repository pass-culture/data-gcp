-- Collective (EAC) bookings by cultural partner (venue) geography, as
-- external_reporting__collective_partner_booking: a department under the
-- individual or collective statistical secret at the last complete month (6 or
-- 48 month window) is masked on every month.
{% set last_month = (
    "date_trunc(date_sub(date('" ~ ds() ~ "'), interval 1 month), month)"
) %}

with
    venue_department_bookings as (
        select
            partition_month,
            scholar_year,
            venue_region_name,
            venue_region_code,
            venue_department_name,
            venue_department_code,
            sum(total_collective_bookings) as total_collective_bookings,
            sum(total_collective_amount_spent) as total_collective_amount_spent
        from {{ ref("metrics_booking__collective") }}
        where
            partition_month <= {{ last_month }}
            and partition_month >= date('2022-01-01')
        group by
            partition_month,
            scholar_year,
            venue_region_name,
            venue_region_code,
            venue_department_name,
            venue_department_code
    ),

    secret_departments as (
        select
            department_code,
            logical_or(is_department_secret_stat) as is_secret,
            -- same wording as the secret_detail of external reporting
            string_agg(
                distinct if(
                    is_department_secret_stat,
                    concat(department_secret_reason_label, ' (', secret_scope, ')'),
                    null
                ),
                ' ; '
                order by
                    if(
                        is_department_secret_stat,
                        concat(department_secret_reason_label, ' (', secret_scope, ')'),
                        null
                    )
            ) as department_secret_reason_label
        from
            (
                {% for secret_model in [
                    "statistical_secret__cultural_partner_individual",
                    "statistical_secret__cultural_partner_collective",
                ] %}
                    {% if not loop.first %}
                        union all
                    {% endif %}
                    select
                        department_code,
                        is_department_secret_stat,
                        department_secret_reason_label,
                        format(
                            '%s, %d mois',
                            '{{ "collectif" if "collective" in secret_model else "individuel" }}',
                            rolling_window_months_count
                        ) as secret_scope
                    from {{ ref(secret_model) }}
                    where
                        date(partition_month) = {{ last_month }}
                        and rolling_window_months_count in (6, 48)
                {% endfor %}
            )
        group by department_code
    )

select
    facts.partition_month,
    facts.scholar_year,
    facts.venue_region_name,
    facts.venue_region_code,
    facts.venue_department_name,
    facts.venue_department_code,
    sec.department_secret_reason_label,
    coalesce(sec.is_secret, false) as is_statistic_secret,
    if(
        coalesce(sec.is_secret, false), null, facts.total_collective_bookings
    ) as total_collective_bookings,
    if(
        coalesce(sec.is_secret, false), null, facts.total_collective_amount_spent
    ) as total_collective_amount_spent
from venue_department_bookings as facts
left join secret_departments as sec on facts.venue_department_code = sec.department_code
