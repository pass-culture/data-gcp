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
    {"name": "NAT", "value_expr": "'NAT'", "value_filter": "'NAT'"},
    {
        "name": "REG",
        "value_expr": "institution_region_name",
        "value_filter": "venue_region_name",
    },
    {
        "name": "ACAD",
        "value_expr": "institution_academy_name",
        "value_filter": "venue_academy_name",
    },
    {
        "name": "COM",
        "value_expr": "institution_city",
        "value_filter": "venue_city",
    },
    {
        "name": "EPCI",
        "value_expr": "institution_epci",
        "value_filter": "venue_epci",
    },
] %}

with
    instit_amount as (
        select
            ea.scholar_year,
            ey.educational_year_beginning_date,
            ey.educational_year_expiration_date,
            ea.institution_region_name as region_name,
            ea.institution_academy_name as institution_academie,
            ea.institution_city,
            ea.institution_epci,
            sum(ea.total_scholar_year_deposit) as total_institution_deposit_amount
        from {{ ref("eple_aggregated") }} as ea
        left join
            {{ source("raw", "applicative_database_educational_year") }} as ey
            on ea.scholar_year = ey.scholar_year
        group by
            ea.scholar_year,
            ey.educational_year_beginning_date,
            ey.educational_year_expiration_date,
            ea.institution_region_name,
            ea.institution_academy_name,
            ea.institution_city,
            ea.institution_epci
    ),

    -- 1. Toutes les combinaisons de dimensions qui ont eu des réservations
    all_dimensions as (
        select distinct
            scholar_year,
            institution_region_name,
            institution_academy_name,
            institution_city,
            institution_epci,
            venue_region_name,
            venue_academy_name,
            venue_city,
            venue_epci
        from {{ ref("mrt_global__collective_booking") }}
        where
            collective_booking_status = 'REIMBURSED'
            and scholar_year is not null
            and institution_region_name is not null
            and institution_academy_name is not null
            and institution_city is not null
            and institution_epci is not null
            and venue_region_name is not null
            and venue_academy_name is not null
            and venue_city is not null
            and venue_epci is not null
    ),

    -- 2. Tous les mois où il y a eu des réservations pour chaque année scolaire
    all_months as (
        select distinct
            gcb.scholar_year,
            date_trunc(date(gcb.collective_booking_used_date), month) as partition_month
        from {{ ref("mrt_global__collective_booking") }} as gcb
        left join
            {{ source("raw", "applicative_database_educational_year") }} as ey
            on gcb.scholar_year = ey.scholar_year
        where
            gcb.collective_booking_status = 'REIMBURSED'
            and gcb.scholar_year is not null
            and gcb.collective_booking_used_date >= ey.educational_year_beginning_date
            and gcb.collective_booking_used_date <= ey.educational_year_expiration_date
    ),

    -- 3. Grille complète dimensions x mois
    complete_grid as (
        select
            ad.scholar_year,
            ad.institution_region_name,
            ad.institution_academy_name,
            ad.institution_city,
            ad.institution_epci,
            ad.venue_region_name,
            ad.venue_academy_name,
            ad.venue_city,
            ad.venue_epci,
            am.partition_month
        from all_dimensions as ad
        inner join all_months as am on ad.scholar_year = am.scholar_year
    ),

    -- 4. Données mensuelles agrégées
    monthly_data as (
        select
            scholar_year,
            institution_region_name,
            institution_academy_name,
            institution_city,
            institution_epci,
            venue_region_name,
            venue_academy_name,
            venue_city,
            venue_epci,
            date_trunc(date(collective_booking_used_date), month) as partition_month,
            sum(booking_amount) as total_amount_spent_reimbursed,
            count(distinct collective_booking_id) as total_collective_bookings
        from {{ ref("mrt_global__collective_booking") }}
        where collective_booking_status = 'REIMBURSED'
        group by
            scholar_year,
            institution_region_name,
            institution_academy_name,
            institution_city,
            institution_epci,
            venue_region_name,
            venue_academy_name,
            venue_city,
            venue_epci,
            partition_month
    ),

    -- 5. Jointure complète avec remplacement des NULL par 0
    complete_data as (
        select
            cg.scholar_year,
            cg.institution_region_name,
            cg.institution_academy_name,
            cg.institution_city,
            cg.institution_epci,
            cg.venue_region_name,
            cg.venue_academy_name,
            cg.venue_city,
            cg.venue_epci,
            cg.partition_month,
            coalesce(
                md.total_amount_spent_reimbursed, 0
            ) as total_amount_spent_reimbursed,
            coalesce(md.total_collective_bookings, 0) as total_collective_bookings
        from complete_grid as cg
        left join
            monthly_data as md
            on cg.scholar_year = md.scholar_year
            and cg.institution_region_name = md.institution_region_name
            and cg.institution_academy_name = md.institution_academy_name
            and cg.institution_city = md.institution_city
            and cg.institution_epci = md.institution_epci
            and cg.venue_region_name = md.venue_region_name
            and cg.venue_academy_name = md.venue_academy_name
            and cg.venue_city = md.venue_city
            and cg.venue_epci = md.venue_epci
            and cg.partition_month = md.partition_month
    ),

    booking_cumul_amount as (
        -- 6. Calcul du cumulé sur la grille complète
        select
            partition_month,
            venue_region_name,
            scholar_year,
            venue_academy_name,
            institution_region_name,
            institution_academy_name,
            institution_city,
            institution_epci,
            venue_city,
            venue_epci,
            total_amount_spent_reimbursed,
            sum(total_amount_spent_reimbursed) over (
                partition by
                    scholar_year,
                    institution_region_name,
                    institution_academy_name,
                    institution_city,
                    institution_epci,
                    venue_region_name,
                    venue_academy_name,
                    venue_city,
                    venue_epci
                order by partition_month
                rows unbounded preceding
            ) as cumulative_amount_spent,
            sum(total_collective_bookings) over (
                partition by
                    scholar_year,
                    institution_region_name,
                    institution_academy_name,
                    institution_city,
                    institution_epci,
                    venue_region_name,
                    venue_academy_name,
                    venue_city,
                    venue_epci
                order by partition_month
                rows unbounded preceding
            ) as cumulative_bookings
        from complete_data
        order by
            scholar_year,
            institution_region_name,
            institution_academy_name,
            institution_city,
            institution_epci,
            venue_region_name,
            venue_academy_name,
            venue_city,
            venue_epci,
            partition_month
    ),

    base_data as (
        select
            ia.institution_academie as institution_academy_name,
            ia.region_name as institution_region_name,
            ia.institution_city,
            ia.institution_epci,
            cb.venue_academy_name,
            cb.venue_region_name,
            cb.venue_city,
            cb.venue_epci,
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
            and ia.institution_city = cb.institution_city
            and ia.institution_epci = cb.institution_epci
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
                coalesce(sum(cumulative_amount_spent), 0) as numerator,
                coalesce(
                    sum(distinct total_institution_deposit_amount), 0
                ) as denominator,
                -- On prend ici sum sur les distinct total_institution_deposit_amount
                -- le total_institution_deposit_amount de chaque institution est
                -- duppliqué pour chaque combinaison de (venue_region_name,
                -- venue_academy_name et partition_month)
                -- Ce fix fonctionne car chaque institution a un deposit amount
                -- différent
                -- Dette technique à corriger lors de l'amélioration du projet
                safe_divide(
                    coalesce(sum(cumulative_amount_spent), 0),
                    coalesce(sum(distinct total_institution_deposit_amount), 0)
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
                coalesce(sum(cumulative_amount_spent), 0) as numerator,
                coalesce(sum(cumulative_bookings), 0) as denominator,
                safe_divide(
                    coalesce(sum(cumulative_amount_spent), 0),
                    coalesce(sum(cumulative_bookings), 0)
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
                'montant_moyen_par_reservation_meme_territoire' as kpi_name,
                coalesce(sum(cumulative_amount_spent), 0) as numerator,
                coalesce(sum(cumulative_bookings), 0) as denominator,
                safe_divide(
                    coalesce(sum(cumulative_amount_spent), 0),
                    coalesce(sum(cumulative_bookings), 0)
                ) as kpi
            from base_data
            where
                {{ dim.value_filter }} = {{ dim.value_expr }}
                {% if is_incremental() %}
                    and partition_month
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
