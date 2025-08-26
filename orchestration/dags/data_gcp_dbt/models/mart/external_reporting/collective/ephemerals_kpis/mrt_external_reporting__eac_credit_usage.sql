{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{% set dimensions = [
    {"name": "NAT", "value_expr": "'NAT'"},
    {"name": "REG", "value_expr": "institution_region_name"},
    {"name": "ACAD", "value_expr": "institution_academy_name"},
] %}

with
    instit_amount as (
        select
            ea.scholar_year,
            ey.educational_year_beginning_date,
            ey.educational_year_expiration_date,
            ea.region_name,
            ea.institution_academie,
            sum(ea.institution_deposit_amount) as total_institution_deposit_amount
        from {{ ref("eple_aggregated") }} as ea
        left join
            {{ source("raw", "applicative_database_educational_year") }} as ey
            on ea.scholar_year = ey.scholar_year
        group by
            ea.scholar_year,
            ey.educational_year_beginning_date,
            ey.educational_year_expiration_date,
            region_name,
            institution_academie
    ),

    collective_booking_data as (
        select
            institution_region_name,
            institution_academy_name,
            scholar_year,
            date_trunc(date(collective_booking_used_date), month) as partition_month,
            sum(booking_amount) as total_amount_spent_reimbursed,
            count(distinct collective_booking_id) as total_collective_bookings
        from {{ ref("mrt_global__collective_booking") }}
        where collective_booking_status = 'REIMBURSED'
        group by
            institution_region_name,
            institution_academy_name,
            scholar_year,
            partition_month
    ),

    booking_cumul_amount as (
        select
            institution_region_name,
            institution_academy_name,
            scholar_year,
            partition_month,
            -- cumul par année scolaire
            sum(total_amount_spent_reimbursed) over (
                partition by
                    institution_region_name, institution_academy_name, scholar_year
                order by partition_month
                rows unbounded preceding
            ) as cumulative_amount_spent,
            sum(total_collective_bookings) over (
                partition by
                    institution_region_name, institution_academy_name, scholar_year
                order by partition_month
                rows unbounded preceding
            ) as cumulative_bookings
        from collective_booking_data
    ),

    base_data as (
        select
            ia.institution_academie as institution_academy_name,
            ia.region_name as institution_region_name,
            ia.scholar_year,
            cb.partition_month,
            ia.total_institution_deposit_amount,
            cb.cumulative_amount_spent,
            cb.cumulative_bookings
        from instit_amount as ia
        left join
            booking_cumul_amount as cb
            on ia.scholar_year = cb.scholar_year
            and ia.region_name = cb.institution_region_name
            and ia.institution_academie = cb.institution_academy_name
        where
            ia.educational_year_beginning_date <= cb.partition_month
            and ia.educational_year_expiration_date >= cb.partition_month
    ),

    aggregation_by_scholar_year as (
        {% for dim in dimensions %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                partition_month,
                scholar_year,
                timestamp("{{ ts() }}") as updated_at,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                'taux_d_utilisation_du_credit' as kpi_name,
                sum(cumulative_amount_spent) as numerator,
                sum(total_institution_deposit_amount) as denominator,
                safe_divide(
                    sum(cumulative_amount_spent), sum(total_institution_deposit_amount)
                ) as kpi
            from base_data
            {% if is_incremental() %}
                where
                    partition_month
                    = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
            {% endif %}
            group by
                partition_month,
                scholar_year,
                updated_at,
                dimension_name,
                dimension_value
            union all
            select
                partition_month,
                scholar_year,
                timestamp("{{ ts() }}") as updated_at,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                'montant_moyen_par_reservation' as kpi_name,
                sum(cumulative_amount_spent) as numerator,
                sum(cumulative_bookings) as denominator,
                safe_divide(
                    sum(cumulative_amount_spent), sum(cumulative_bookings)
                ) as kpi
            from base_data
            {% if is_incremental() %}
                where
                    partition_month
                    = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
            {% endif %}
            group by
                partition_month,
                scholar_year,
                updated_at,
                dimension_name,
                dimension_value
        {% endfor %}
    )

select
    partition_month,
    updated_at,
    dimension_name,
    dimension_value,
    kpi_name,
    numerator,
    denominator,
    kpi
from aggregation_by_scholar_year
