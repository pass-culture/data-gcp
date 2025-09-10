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
    {"name": "REG", "value_expr": "region_name"},
    {"name": "DEP", "value_expr": "dep_name"},
] %}

{% set activity_list = [
    {"activity": "Apprenti", "value": "apprenti"},
    {"activity": "Chômeur, En recherche d'emploi", "value": "chomeur"},
    {
        "activity": "Volontaire en service civique rémunéré",
        "value": "volontaire",
    },
    {"activity": "Alternant", "value": "alternant"},
    {"activity": "Employé", "value": "employe"},
    {"activity": "Etudiant", "value": "etudiant"},
    {"activity": "Lycéen", "value": "lyceen"},
    {"activity": "Collégien", "value": "collegien"},
    {
        "activity": "Inactif (ni en emploi ni au chômage), En incapacité de travailler",
        "value": "inactif",
    },
] %}

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
        {% for dim in dimensions %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                partition_month,
                updated_at,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                "beneficiaire_actuel" as kpi_name,
                count(distinct user_id) as numerator,
                1 as denominator
            from active_users_base
            group by
                partition_month, updated_at, dimension_name, dimension_value, kpi_name
        {% endfor %}

        union all

        -- Bénéficiaires totaux
        {% for dim in dimensions %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                partition_month,
                updated_at,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                "beneficiaire_total" as kpi_name,
                count(distinct user_id) as numerator,
                1 as denominator
            from total_users_base
            group by
                partition_month, updated_at, dimension_name, dimension_value, kpi_name
        {% endfor %}

        union all

        -- QPV
        {% for dim in dimensions %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                partition_month,
                updated_at,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                "pct_beneficiaire_actuel_qpv" as kpi_name,
                count(distinct case when user_is_in_qpv then user_id end) as numerator,
                count(distinct user_id) as denominator
            from active_users_base
            group by
                partition_month, updated_at, dimension_name, dimension_value, kpi_name
        {% endfor %}

        union all

        -- Rural
        {% for dim in dimensions %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                partition_month,
                updated_at,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                "pct_beneficiaire_actuel_rural" as kpi_name,
                count(
                    distinct case
                        when user_macro_density_label = "rural" then user_id
                    end
                ) as numerator,
                count(distinct user_id) as denominator
            from active_users_base
            group by
                partition_month, updated_at, dimension_name, dimension_value, kpi_name
        {% endfor %}

        union all

        -- Non scolarisé
        {% for dim in dimensions %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                partition_month,
                updated_at,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                "pct_beneficiaire_actuel_non_scolarise" as kpi_name,
                count(
                    distinct case when not user_is_in_education then user_id end
                ) as numerator,
                count(distinct user_id) as denominator
            from active_users_base
            group by
                partition_month, updated_at, dimension_name, dimension_value, kpi_name
        {% endfor %}

        union all

        -- Activités
        {% for activity in activity_list %}
            {% if not loop.first %}
                union all
            {% endif %}
            {% for dim in dimensions %}
                {% if not loop.first %}
                    union all
                {% endif %}
                select
                    partition_month,
                    updated_at,
                    '{{ dim.name }}' as dimension_name,
                    {{ dim.value_expr }} as dimension_value,
                    "pct_beneficiaire_actuel_{{ activity.value }}" as kpi_name,
                    count(
                        distinct case
                            when user_activity = "{{ activity.activity }}" then user_id
                        end
                    ) as numerator,
                    count(distinct user_id) as denominator
                from active_users_base
                group by
                    partition_month,
                    updated_at,
                    dimension_name,
                    dimension_value,
                    kpi_name
            {% endfor %}
        {% endfor %}

        union all

        -- Genre
        {% for dim in dimensions %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                partition_month,
                updated_at,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                "beneficiaire_actuel_homme" as kpi_name,
                count(
                    distinct case when user_civility = "M." then user_id end
                ) as numerator,
                1 as denominator
            from active_users_base
            group by
                partition_month, updated_at, dimension_name, dimension_value, kpi_name
        {% endfor %}

        union all

        {% for dim in dimensions %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                partition_month,
                updated_at,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                "beneficiaire_actuel_femme" as kpi_name,
                count(
                    distinct case when user_civility = "Mme." then user_id end
                ) as numerator,
                1 as denominator
            from active_users_base
            group by
                partition_month, updated_at, dimension_name, dimension_value, kpi_name
        {% endfor %}

        union all

        {% for dim in dimensions %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                partition_month,
                updated_at,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                "beneficiaire_actuel_sans_genre" as kpi_name,
                count(
                    distinct case when user_civility is null then user_id end
                ) as numerator,
                1 as denominator
            from active_users_base
            group by
                partition_month, updated_at, dimension_name, dimension_value, kpi_name
        {% endfor %}
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
