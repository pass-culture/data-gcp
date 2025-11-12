{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{% set dimensions = get_dimensions(none, 'geo') %}
{% set activity_list = get_activity_list() %}

-- Base CTEs optimisées
with
    last_day_of_month as (
        select
            date_trunc(deposit_active_date, month) as partition_month,
            timestamp("{{ ts() }}") as updated_at,
            max(deposit_active_date) as last_active_date
        from {{ ref("mrt_native__daily_user_deposit") }}
        where deposit_active_date > date("2021-01-01")
        group by date_trunc(deposit_active_date, month)
    ),

    user_amount_spent_per_day as (
        select
            uua.deposit_active_date,
            uua.user_id,
            uua.deposit_amount,
            case
                when uua.deposit_type = "GRANT_17_18" and uua.user_age <= 17
                then "GRANT_15_17"
                when uua.deposit_type = "GRANT_17_18" and uua.user_age >= 18
                then "GRANT_18"
                else uua.deposit_type
            end as deposit_type,
            coalesce(sum(booking_intermediary_amount), 0) as amount_spent
        from {{ ref("mrt_native__daily_user_deposit") }} as uua
        left join
            {{ ref("mrt_global__booking") }} as ebd
            on uua.deposit_id = ebd.deposit_id
            and uua.deposit_active_date = date(booking_used_date)
            and booking_is_used
        where deposit_active_date > date("2021-01-01")
        group by deposit_active_date, user_id, deposit_type, deposit_amount
    ),

    user_cumulative_amount_spent as (
        select
            deposit_active_date,
            user_id,
            deposit_type,
            deposit_amount as initial_deposit_amount,
            sum(amount_spent) over (
                partition by user_id, deposit_type order by deposit_active_date asc
            ) as cumulative_amount_spent
        from user_amount_spent_per_day
    ),

    -- Table principale avec tous les utilisateurs actifs et leurs informations
    active_users_base as (
        select
            uua.user_id,
            ldm.partition_month,
            ldm.updated_at,
            uua.initial_deposit_amount,
            uua.cumulative_amount_spent,
            eud.user_is_in_qpv,
            eud.user_macro_density_label,
            eud.user_is_in_education,
            eud.user_activity,
            eud.user_civility,
            eud.first_deposit_creation_date,
            rd.region_name,
            rd.dep_name
        from user_cumulative_amount_spent as uua
        inner join
            last_day_of_month as ldm on uua.deposit_active_date = ldm.last_active_date
        inner join {{ ref("mrt_global__user") }} as eud on uua.user_id = eud.user_id
        left join
            {{ ref("region_department") }} as rd
            on eud.user_department_code = rd.num_dep
        where
            cumulative_amount_spent < initial_deposit_amount
            {% if is_incremental() %}
                and partition_month
                = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
            {% endif %}
    ),

    -- Table pour les bénéficiaires totaux
    total_users_base as (
        select
            eud.user_id,
            ldm.partition_month,
            ldm.updated_at,
            eud.first_deposit_creation_date,
            rd.region_name,
            rd.dep_name
        from last_day_of_month as ldm
        inner join
            {{ ref("mrt_global__user") }} as eud
            on date(eud.first_deposit_creation_date) <= date(ldm.last_active_date)
        left join
            {{ ref("region_department") }} as rd
            on eud.user_department_code = rd.num_dep
        {% if is_incremental() %}
            where
                partition_month
                = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
        {% endif %}
    ),

    -- Agrégation unifiée pour tous les KPIs
    unified_metrics as (
        -- Bénéficiaires actuels
        {{
            generate_metric_by_dimensions(
                "active_users_base",
                "beneficiaire_actuel",
                "count(distinct user_id)",
                "1",
                dimensions,
            )
        }}

        union all

        -- Bénéficiaires totaux
        {{
            generate_metric_by_dimensions(
                "total_users_base",
                "beneficiaire_total",
                "count(distinct user_id)",
                "1",
                dimensions,
            )
        }}

        union all

        -- QPV
        {{
            generate_metric_by_dimensions(
                "active_users_base",
                "pct_beneficiaire_actuel_qpv",
                "count(distinct case when user_is_in_qpv then user_id end)",
                "count(distinct user_id)",
                dimensions,
            )
        }}

        union all

        -- Rural
        {{
            generate_metric_by_dimensions(
                "active_users_base",
                "pct_beneficiaire_actuel_rural",
                'count(distinct case when user_macro_density_label = "rural" then user_id end)',
                "count(distinct user_id)",
                dimensions,
            )
        }}

        union all

        -- Non scolarisé
        {{
            generate_metric_by_dimensions(
                "active_users_base",
                "pct_beneficiaire_actuel_non_scolarise",
                "count(distinct case when not user_is_in_education then user_id end)",
                "count(distinct user_id)",
                dimensions,
            )
        }}

        union all

        -- Activités
        {{ generate_activity_metrics("active_users_base", activity_list, dimensions) }}

        union all

        -- Genre
        {{
            generate_metric_by_dimensions(
                "active_users_base",
                "beneficiaire_actuel_homme",
                'count(distinct case when user_civility = "M." then user_id end)',
                "1",
                dimensions,
            )
        }}

        union all

        {{
            generate_metric_by_dimensions(
                "active_users_base",
                "beneficiaire_actuel_femme",
                'count(distinct case when user_civility = "Mme." then user_id end)',
                "1",
                dimensions,
            )
        }}

        union all

        {{
            generate_metric_by_dimensions(
                "active_users_base",
                "beneficiaire_actuel_sans_genre",
                "count(distinct case when user_civility is null then user_id end)",
                "1",
                dimensions,
            )
        }}
    )

select
    partition_month,
    updated_at,
    dimension_name,
    dimension_value,
    kpi_name,
    coalesce(numerator, 0) as numerator,
    coalesce(denominator, 0) as denominator,
    coalesce(safe_divide(numerator, denominator), 0) as kpi
from unified_metrics
