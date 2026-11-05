-- depends_on: {{ ref('mrt_global__educational_deposit') }}
{% set geo_columns = [
    "institution_region_name",
    "institution_academy_name",
    "institution_department_name",
    "institution_epci_code",
    "institution_city_code",
    "institution_municipality_code",
] %}

with
    static_months as (
        select cast(date_month as date) as month_date
        from
            (
                {{
                    dbt_utils.date_spine(
                        datepart="month",
                        start_date="cast('2021-01-01' as date)",
                        end_date="current_date()",
                    )
                }}
            )
    ),

    year_mapping as (
        select
            educational_year_id,
            scholar_year,
            educational_year_beginning_date,
            educational_year_expiration_date
        from {{ source("raw", "applicative_database_educational_year") }}
    ),

    months_spine as (
        select m.month_date as partition_month, y.educational_year_id, y.scholar_year
        from year_mapping as y
        inner join
            static_months as m
            on m.month_date
            between y.educational_year_beginning_date
            and y.educational_year_expiration_date
    ),

    institution_monthly_base as (
        select
            ms.partition_month,
            ms.scholar_year,
            dep.institution_id,
            dep.educational_deposit_amount,
            inst.institution_region_name,
            inst.institution_academy_name,
            inst.institution_department_name,
            inst.institution_epci_code,
            inst.institution_city_code,
            inst.institution_municipality_code
        from {{ ref("mrt_global__educational_deposit") }} as dep
        inner join
            months_spine as ms on dep.educational_year_id = ms.educational_year_id
        left join
            {{ ref("mrt_global__educational_institution") }} as inst
            on dep.institution_id = inst.institution_id
    ),

    first_bookings as (
        select
            educational_institution_id as institution_id,
            scholar_year,
            min(
                date_trunc(collective_booking_creation_date, month)
            ) as first_booking_month
        from {{ ref("mrt_global__collective_booking") }}
        where collective_booking_status != 'CANCELLED'
        group by institution_id, scholar_year
    ),

    coverage as (
        select
            base.partition_month,
            base.scholar_year,
            {% for col in geo_columns %} base.{{ col }}, {% endfor %}
            count(distinct base.institution_id) as total_institutions,
            count(
                distinct case
                    when fb.first_booking_month <= base.partition_month
                    then base.institution_id
                end
            ) as total_engaged_institutions,
            -- credit granted for the whole scholar year (all its deposits),
            -- repeated on every month of the scholar year
            sum(base.educational_deposit_amount) as total_deposit
        from institution_monthly_base as base
        left join
            first_bookings as fb
            on base.institution_id = fb.institution_id
            and base.scholar_year = fb.scholar_year
        group by base.partition_month, base.scholar_year, {{ geo_columns | join(", ") }}
    ),

    -- institutions credited at least once since the generalisation, counted from
    -- the start of their first credited scholar year, whether or not they are
    -- credited in the current one
    first_credits as (
        select
            dep.institution_id,
            min(
                date_trunc(date(y.educational_year_beginning_date), month)
            ) as first_credit_month
        from {{ ref("mrt_global__educational_deposit") }} as dep
        inner join year_mapping as y on dep.educational_year_id = y.educational_year_id
        group by dep.institution_id
    ),

    ever_credited as (
        select
            months.partition_month,
            months.scholar_year,
            {% for col in geo_columns %} inst.{{ col }}, {% endfor %}
            count(distinct fc.institution_id) as total_ever_credited_institutions
        from (select distinct partition_month, scholar_year from months_spine) as months
        inner join
            first_credits as fc on months.partition_month >= fc.first_credit_month
        left join
            {{ ref("mrt_global__educational_institution") }} as inst
            on fc.institution_id = inst.institution_id
        group by
            months.partition_month, months.scholar_year, {{ geo_columns | join(", ") }}
    ),

    -- a territory can have institutions credited in the past but none in the
    -- current scholar year: keep it, with no credited institution this year
    coverage_keyed as (
        select *, to_json_string(struct({{ geo_columns | join(", ") }})) as geo_key
        from coverage
    ),

    ever_credited_keyed as (
        select *, to_json_string(struct({{ geo_columns | join(", ") }})) as geo_key
        from ever_credited
    )

select
    coalesce(c.partition_month, e.partition_month) as partition_month,
    coalesce(c.scholar_year, e.scholar_year) as scholar_year,
    {% for col in geo_columns %}
        coalesce(c.{{ col }}, e.{{ col }}) as {{ col }},
    {% endfor %}
    coalesce(c.total_institutions, 0) as total_institutions,
    coalesce(c.total_engaged_institutions, 0) as total_engaged_institutions,
    coalesce(c.total_deposit, 0) as total_deposit,
    coalesce(e.total_ever_credited_institutions, 0) as total_ever_credited_institutions
from coverage_keyed as c
full outer join
    ever_credited_keyed as e
    on c.partition_month = e.partition_month
    and c.scholar_year = e.scholar_year
    and c.geo_key = e.geo_key
