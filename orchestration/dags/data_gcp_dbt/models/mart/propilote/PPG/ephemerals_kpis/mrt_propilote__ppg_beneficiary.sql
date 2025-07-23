{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "execution_date", "data_type": "date"},
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

-- noqa: disable=all
with
    last_day_of_month as (
        select
            date_trunc(deposit_active_date, month) as execution_date,
            date("{{ ds() }}") as update_date,
            max(deposit_active_date) as last_active_date
        from {{ ref("mrt_native__daily_user_deposit") }}
        where
            deposit_active_date > date('2021-01-01')
            {% if is_incremental() %}
                and date_trunc(deposit_active_date, month)
                = date_trunc(date("{{ ds() }}"), month)
            {% endif %}
        group by date_trunc(deposit_active_date, month)
    ),

    user_amount_spent_per_day as (
        select
            uua.deposit_active_date,
            uua.user_id,
            uua.deposit_amount,
            coalesce(sum(booking_intermediary_amount), 0) as amount_spent
        from {{ ref("mrt_native__daily_user_deposit") }} uua
        left join
            {{ ref("mrt_global__booking") }} ebd
            on ebd.deposit_id = uua.deposit_id
            and uua.deposit_active_date = date(booking_used_date)
            and booking_is_used
        where deposit_active_date > date('2021-01-01')
        group by deposit_active_date, user_id, deposit_amount
    ),

    user_cumulative_amount_spent as (
        select
            deposit_active_date,
            user_id,
            deposit_amount as initial_deposit_amount,
            sum(amount_spent) over (
                partition by user_id order by deposit_active_date asc
            ) as cumulative_amount_spent
        from user_amount_spent_per_day
        {% if is_incremental() %}
            where deposit_active_date = date_trunc(date("{{ ds() }}"), month)
        {% endif %}
    ),

    aggregated_active_beneficiary as (
        {% for dim in dimensions %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                execution_date,
                update_date,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                'beneficiaire_actuel' as kpi_name,
                count(distinct uua.user_id) as numerator,
                1 as denominator
            from user_cumulative_amount_spent uua
            inner join
                last_day_of_month ldm on ldm.last_active_date = uua.deposit_active_date
            inner join {{ ref("mrt_global__user") }} eud on eud.user_id = uua.user_id
            left join
                {{ ref("region_department") }} rd
                on eud.user_department_code = rd.num_dep
            where
                cumulative_amount_spent < initial_deposit_amount
                {% if is_incremental() %}
                    and execution_date = date_trunc(date("{{ ds() }}"), month)
                {% endif %}
            group by
                execution_date, update_date, dimension_name, dimension_value, kpi_name
        {% endfor %}
    ),

    aggregated_total_beneficiary as (
        {% for dim in dimensions %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                execution_date,
                update_date,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                "beneficiaire_total" as kpi_name,
                count(distinct eud.user_id) as numerator,
                1 as denominator
            from last_day_of_month ldm
            inner join
                {{ ref("mrt_global__user") }} eud
                on date(eud.first_deposit_creation_date) <= date(ldm.last_active_date)
            left join
                {{ ref("region_department") }} as rd
                on eud.user_department_code = rd.num_dep
            {% if is_incremental() %}
                where execution_date = date_trunc(date("{{ ds() }}"), month)
            {% endif %}
            group by
                execution_date, update_date, dimension_name, dimension_value, kpi_name
        {% endfor %}
    ),

    aggregated_active_qpv_beneficiary as (
        {% for dim in dimensions %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                execution_date,
                update_date,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                'pct_beneficiaire_actuel_qpv' as kpi_name,
                count(
                    distinct case when eud.user_is_in_qpv then uua.user_id end
                ) as numerator,
                count(distinct uua.user_id) as denominator
            from user_cumulative_amount_spent uua
            inner join
                last_day_of_month ldm on ldm.last_active_date = uua.deposit_active_date
            inner join {{ ref("mrt_global__user") }} eud on eud.user_id = uua.user_id
            left join
                {{ ref("region_department") }} rd
                on eud.user_department_code = rd.num_dep
            where
                cumulative_amount_spent < initial_deposit_amount
                {% if is_incremental() %}
                    and execution_date = date_trunc(date("{{ ds() }}"), month)
                {% endif %}
            group by
                execution_date, update_date, dimension_name, dimension_value, kpi_name
        {% endfor %}
    ),

    aggregated_active_rural_beneficiary as (
        {% for dim in dimensions %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                execution_date,
                update_date,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                "pct_beneficiaire_actuel_rural" as kpi_name,
                count(
                    distinct case
                        when eud.user_macro_density_label = "rural" then uua.user_id
                    end
                ) as numerator,
                count(distinct uua.user_id) as denominator
            from user_cumulative_amount_spent uua
            inner join
                last_day_of_month ldm on ldm.last_active_date = uua.deposit_active_date
            inner join {{ ref("mrt_global__user") }} eud on eud.user_id = uua.user_id
            left join
                {{ ref("region_department") }} rd
                on eud.user_department_code = rd.num_dep
            where
                cumulative_amount_spent < initial_deposit_amount
                {% if is_incremental() %}
                    and execution_date = date_trunc(date("{{ ds() }}"), month)
                {% endif %}
            group by
                execution_date, update_date, dimension_name, dimension_value, kpi_name
        {% endfor %}
    ),

    aggregated_active_not_in_education_beneficiary as (
        {% for dim in dimensions %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                execution_date,
                update_date,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                "pct_beneficiaire_actuel_non_scolarise" as kpi_name,
                count(
                    distinct case when not eud.user_is_in_education then uua.user_id end
                ) as numerator,
                count(distinct uua.user_id) as denominator
            from user_cumulative_amount_spent uua
            inner join
                last_day_of_month ldm on ldm.last_active_date = uua.deposit_active_date
            inner join {{ ref("mrt_global__user") }} eud on eud.user_id = uua.user_id
            left join
                {{ ref("region_department") }} rd
                on eud.user_department_code = rd.num_dep
            where
                cumulative_amount_spent < initial_deposit_amount
                {% if is_incremental() %}
                    and execution_date = date_trunc(date("{{ ds() }}"), month)
                {% endif %}
            group by
                execution_date, update_date, dimension_name, dimension_value, kpi_name
        {% endfor %}
    )
    {% for activity in activity_list %}
        ,

        aggregated_active_{{ activity.value }}_beneficiary as (
            {% for dim in dimensions %}
                {% if not loop.first %}
                    union all
                {% endif %}
                select
                    execution_date,
                    update_date,
                    '{{ dim.name }}' as dimension_name,
                    {{ dim.value_expr }} as dimension_value,
                    "pct_beneficiaire_actuel_{{ activity.value }}" as kpi_name,
                    count(
                        distinct case
                            when eud.user_activity = "{{ activity.activity }}"
                            then uua.user_id
                        end
                    ) as numerator,
                    count(distinct uua.user_id) as denominator
                from user_cumulative_amount_spent uua
                inner join
                    last_day_of_month ldm
                    on ldm.last_active_date = uua.deposit_active_date
                inner join
                    {{ ref("mrt_global__user") }} eud on eud.user_id = uua.user_id
                left join
                    {{ ref("region_department") }} rd
                    on eud.user_department_code = rd.num_dep
                where
                    cumulative_amount_spent < initial_deposit_amount
                    {% if is_incremental() %}
                        and execution_date = date_trunc(date("{{ ds() }}"), month)
                    {% endif %}
                group by
                    execution_date,
                    update_date,
                    dimension_name,
                    dimension_value,
                    kpi_name
            {% endfor %}
        )
    {% endfor %}

{% set cte_list = [
    'aggregated_active_beneficiary',
    'aggregated_total_beneficiary',
    'aggregated_active_qpv_beneficiary',
    'aggregated_active_rural_beneficiary',
    'aggregated_active_not_in_education_beneficiary',
    'aggregated_active_apprenti_beneficiary',
    'aggregated_active_chomeur_beneficiary',
    'aggregated_active_volontaire_beneficiary',
    'aggregated_active_alternant_beneficiary',
    'aggregated_active_employe_beneficiary',
    'aggregated_active_etudiant_beneficiary',
    'aggregated_active_lyceen_beneficiary',
    'aggregated_active_collegien_beneficiary',
    'aggregated_active_inactif_beneficiary',
] %}

{% for cte in cte_list %}
{% if not loop.first %}
union all
{% endif %}
select
    execution_date,
    update_date,
    dimension_name,
    dimension_value,
    kpi_name,
    numerator,
    denominator,
    safe_divide(numerator, denominator) as kpi
from {{ cte_list }}
{% endfor %}