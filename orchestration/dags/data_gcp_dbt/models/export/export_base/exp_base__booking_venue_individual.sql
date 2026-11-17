{#
    Individual bookings used in the month and their amounts, by venue
    (cultural partner) territory, at every published level (DEP, REG, GROUP,
    NAT), under the statistical secret of businesses: see venue_secret_levels.
    Same source and months as external_reporting__finance (all categories):
    2022 to the month before the run date.
#}
{% set last_month = (
    "date_trunc(date_sub(date('" ~ ds() ~ "'), interval 1 month), month)"
) %}
{% set measures = [
    "total_bookings",
    "total_quantities",
    "total_revenue_amount",
    "total_reimbursed_amount",
    "total_contribution_amount",
] %}

with
    department_facts as (
        select
            partition_month,
            venue_department_code as department_code,
            {% for m in measures %}
                sum({{ m }}) as {{ m }}{% if not loop.last %},{% endif %}
            {% endfor %}
        from {{ ref("metrics_booking__finance_individual") }}
        where partition_month between date('2022-01-01') and {{ last_month }}
        group by partition_month, venue_department_code
    )
    {{ venue_secret_levels("department_facts", measures) }}
