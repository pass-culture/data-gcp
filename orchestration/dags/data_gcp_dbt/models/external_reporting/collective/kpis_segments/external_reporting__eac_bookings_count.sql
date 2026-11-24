{{ config(materialized="table") }}

{#
    Collective bookings of the schools of a territory (school geography) over
    the last 12 complete months. Used by external_reporting__eac to mask the
    school side KPIs (amounts, averages, domain and format breakdowns) that
    fewer than min_bookings bookings account for: with 1 to 4 bookings they
    disclose single transactions. Counts of schools are left visible.
#}
{% set min_bookings = 5 %}
{% set last_month = (
    "date_trunc(date_sub(date('" ~ ds() ~ "'), interval 1 month), month)"
) %}

with
    bookings as (
        select dimension_name, dimension_value, sum(numerator) as total_bookings
        from {{ ref("external_reporting__eac_offer_booking") }}
        where
            kpi_name = 'total_reservations_collectives_etablissements'
            and partition_month
            between date_sub({{ last_month }}, interval 11 month) and {{ last_month }}
        group by dimension_name, dimension_value
    ),

    masked_kpis as (
        select distinct kpi_name
        from {{ ref("external_reporting__eac_offer_booking") }}
        where
            ends_with(kpi_name, '_etablissements')
            and not starts_with(kpi_name, 'total_offres_')
        union distinct
        select distinct kpi_name
        from {{ ref("external_reporting__eac_domain") }}
        union distinct
        select distinct kpi_name
        from {{ ref("external_reporting__eac_format") }}
        union distinct
        select distinct kpi_name
        from {{ ref("external_reporting__eac_credit_usage") }}
    )

select
    bookings.dimension_name,
    bookings.dimension_value,
    masked_kpis.kpi_name,
    -- named as expected by apply_statistical_secret(low_count_models)
    bookings.total_bookings as total_beneficiaries,
    {{ min_bookings }} as min_beneficiaries
from bookings
cross join masked_kpis
where bookings.dimension_value is not null
