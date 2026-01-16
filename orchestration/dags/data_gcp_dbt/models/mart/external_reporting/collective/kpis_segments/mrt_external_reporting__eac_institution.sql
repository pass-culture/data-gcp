{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{% set dimensions = get_dimensions("institution", "academic_extended") %}

with recursive
    static_months as (
        -- Génération statique des mois depuis 2021-01-01
        select date('2021-01-01') as month_date
        union all
        select date(date_add(month_date, interval 1 month))
        from static_months
        where month_date < current_date()
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
            dep.institution_department_code,
            dep.institution_academy_name,
            inst.institution_epci,
            inst.institution_city
        from {{ ref("mrt_global__educational_deposit") }} as dep
        left join
            {{ ref("mrt_global__educational_institution") }} as inst
            on dep.institution_id = inst.institution_id
        inner join
            months_spine as ms on dep.educational_year_id = ms.educational_year_id
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

    base_data as (
        select
            base.partition_month,
            base.institution_department_code,
            base.institution_academy_name,
            base.institution_epci,
            base.institution_city,
            count(distinct base.institution_id) as total_institutions,
            count(
                distinct case
                    when fb.first_booking_month <= base.partition_month
                    then base.institution_id
                end
            ) as total_institutions_with_booking
        from institution_monthly_base as base
        left join
            first_bookings as fb
            on base.institution_id = fb.institution_id
            and base.scholar_year = fb.scholar_year
        group by
            base.partition_month,
            base.institution_department_code,
            base.institution_academy_name,
            base.institution_epci,
            base.institution_city
    )

{% for dim in dimensions %}
    {% if not loop.first %}
        union all
    {% endif %}
    select
        partition_month,
        timestamp("{{ ts() }}") as updated_at,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        'taux_participation_eac_eple' as kpi_name,
        sum(total_institutions_with_booking) as numerator,
        sum(total_institutions) as denominator,
        safe_divide(
            sum(total_institutions_with_booking), sum(total_institutions)
        ) as kpi
    from base_data
    {% if is_incremental() %}
        where
            partition_month = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month))
    {% endif %}
    group by partition_month, updated_at, dimension_name, dimension_value, kpi_name
{% endfor %}
