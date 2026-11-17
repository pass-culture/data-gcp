{#
    Collective (EAC) bookings and amounts by venue (cultural partner) territory,
    at every published level (DEP, REG, GROUP, NAT), under the statistical
    secret of businesses: see venue_secret_levels. Same source and months as
    external_reporting__collective_partner_booking: 2022 to the month before
    the run date. Bookings by school territory are not secret (public money):
    see exp_vidoc__booking_collective.
#}
{% set last_month = (
    "date_trunc(date_sub(date('" ~ ds() ~ "'), interval 1 month), month)"
) %}
{% set measures = ["total_collective_bookings", "total_collective_amount_spent"] %}

with
    department_facts as (
        select
            partition_month,
            scholar_year,
            venue_department_code as department_code,
            {% for m in measures %}
                sum({{ m }}) as {{ m }}{% if not loop.last %},{% endif %}
            {% endfor %}
        from {{ ref("metrics_booking__collective") }}
        where partition_month between date('2022-01-01') and {{ last_month }}
        group by partition_month, scholar_year, venue_department_code
    )
    {{ venue_secret_levels("department_facts", measures, ["scholar_year"]) }}
