-- noqa: disable=all
{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{% set activity_list = get_activity_list() %}

-- Optimized version: Single-pass aggregation to avoid memory issues from 68+ UNION
-- ALL statements
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

    -- Active users base with all dimensions pre-computed
    active_users_base as (
        select
            uua.user_id,
            ldm.partition_month,
            ldm.updated_at,
            -- Geographic dimensions
            'NAT' as nat_dim,
            rd.region_name as reg_dim,
            rd.dep_name as dep_dim,
            eud.user_epci_code as epci_dim,
            eud.user_city_code as com_dim,
            -- Segmentation fields
            eud.user_is_in_qpv,
            eud.user_macro_density_label,
            eud.user_is_in_education,
            eud.user_activity,
            eud.user_civility
        from user_cumulative_amount_spent as uua
        inner join
            last_day_of_month as ldm on uua.deposit_active_date = ldm.last_active_date
        inner join
            {{ ref("mrt_global__user_beneficiary") }} as eud
            on uua.user_id = eud.user_id
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

    -- Total users base with all dimensions pre-computed
    total_users_base as (
        select
            eud.user_id,
            ldm.partition_month,
            ldm.updated_at,
            -- Geographic dimensions
            'NAT' as nat_dim,
            rd.region_name as reg_dim,
            rd.dep_name as dep_dim,
            eud.user_epci_code as epci_dim,
            eud.user_city_code as com_dim
        from last_day_of_month as ldm
        inner join
            {{ ref("mrt_global__user_beneficiary") }} as eud
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

    -- Single-pass aggregation for active users - all metrics computed at once per
    -- dimension
    active_users_aggregated as (
        select
            partition_month,
            updated_at,
            nat_dim,
            reg_dim,
            dep_dim,
            epci_dim,
            com_dim,
            -- Base counts
            count(distinct user_id) as total_users,
            -- QPV
            count(distinct case when user_is_in_qpv then user_id end) as qpv_users,
            -- Rural
            count(
                distinct case when user_macro_density_label = "rural" then user_id end
            ) as rural_users,
            -- Non scolarisé
            count(
                distinct case when not user_is_in_education then user_id end
            ) as non_scolarise_users,
            -- Genre
            count(
                distinct case when user_civility = "M." then user_id end
            ) as homme_users,
            count(
                distinct case when user_civility = "Mme." then user_id end
            ) as femme_users,
            count(
                distinct case when user_civility is null then user_id end
            ) as sans_genre_users,
            -- Activities
            {% for activity in activity_list %}
                count(
                    distinct case
                        when user_activity = "{{ activity.activity }}" then user_id
                    end
                ) as {{ activity.value }}_users
                {% if not loop.last %},{% endif %}
            {% endfor %}
        from active_users_base
        group by
            partition_month, updated_at, nat_dim, reg_dim, dep_dim, epci_dim, com_dim
    ),

    -- Single-pass aggregation for total users
    total_users_aggregated as (
        select
            partition_month,
            updated_at,
            nat_dim,
            reg_dim,
            dep_dim,
            epci_dim,
            com_dim,
            count(distinct user_id) as total_users
        from total_users_base
        group by
            partition_month, updated_at, nat_dim, reg_dim, dep_dim, epci_dim, com_dim
    ),

    -- UNPIVOT dimensions for active users metrics
    active_metrics_unpivoted as (
        select
            partition_month,
            updated_at,
            dimension_name,
            dimension_value,
            total_users,
            qpv_users,
            rural_users,
            non_scolarise_users,
            homme_users,
            femme_users,
            sans_genre_users,
            {% for activity in activity_list %}
                {{ activity.value }}_users{% if not loop.last %},{% endif %}
            {% endfor %}
        from
            active_users_aggregated unpivot (
                dimension_value for dimension_name in (
                    nat_dim as 'NAT',
                    reg_dim as 'REG',
                    dep_dim as 'DEP',
                    epci_dim as 'EPCI',
                    com_dim as 'COM'
                )
            )
    ),

    -- Re-aggregate after unpivot to combine rows with same dimension
    active_metrics_final as (
        select
            partition_month,
            updated_at,
            dimension_name,
            dimension_value,
            sum(total_users) as total_users,
            sum(qpv_users) as qpv_users,
            sum(rural_users) as rural_users,
            sum(non_scolarise_users) as non_scolarise_users,
            sum(homme_users) as homme_users,
            sum(femme_users) as femme_users,
            sum(sans_genre_users) as sans_genre_users,
            {% for activity in activity_list %}
                sum({{ activity.value }}_users) as {{ activity.value }}_users
                {% if not loop.last %},{% endif %}
            {% endfor %}
        from active_metrics_unpivoted
        group by partition_month, updated_at, dimension_name, dimension_value
    ),

    -- UNPIVOT dimensions for total users
    total_metrics_unpivoted as (
        select partition_month, updated_at, dimension_name, dimension_value, total_users
        from
            total_users_aggregated unpivot (
                dimension_value for dimension_name in (
                    nat_dim as 'NAT',
                    reg_dim as 'REG',
                    dep_dim as 'DEP',
                    epci_dim as 'EPCI',
                    com_dim as 'COM'
                )
            )
    ),

    total_metrics_final as (
        select
            partition_month,
            updated_at,
            dimension_name,
            dimension_value,
            sum(total_users) as total_users
        from total_metrics_unpivoted
        group by partition_month, updated_at, dimension_name, dimension_value
    ),

    -- Final UNPIVOT of KPIs to get the expected output format
    unified_metrics as (
        -- Bénéficiaires actuels
        select
            partition_month,
            updated_at,
            dimension_name,
            dimension_value,
            "beneficiaire_actuel" as kpi_name,
            total_users as numerator,
            1 as denominator
        from active_metrics_final

        union all

        -- Bénéficiaires totaux
        select
            partition_month,
            updated_at,
            dimension_name,
            dimension_value,
            "beneficiaire_total" as kpi_name,
            total_users as numerator,
            1 as denominator
        from total_metrics_final

        union all

        -- QPV percentage
        select
            partition_month,
            updated_at,
            dimension_name,
            dimension_value,
            "pct_beneficiaire_actuel_qpv" as kpi_name,
            qpv_users as numerator,
            total_users as denominator
        from active_metrics_final

        union all

        -- Rural percentage
        select
            partition_month,
            updated_at,
            dimension_name,
            dimension_value,
            "pct_beneficiaire_actuel_rural" as kpi_name,
            rural_users as numerator,
            total_users as denominator
        from active_metrics_final

        union all

        -- Non scolarisé percentage
        select
            partition_month,
            updated_at,
            dimension_name,
            dimension_value,
            "pct_beneficiaire_actuel_non_scolarise" as kpi_name,
            non_scolarise_users as numerator,
            total_users as denominator
        from active_metrics_final

        union all

        -- Activities
        {% for activity in activity_list %}
            select
                partition_month,
                updated_at,
                dimension_name,
                dimension_value,
                "pct_beneficiaire_actuel_{{ activity.value }}" as kpi_name,
                {{ activity.value }}_users as numerator,
                total_users as denominator
            from active_metrics_final
            {% if not loop.last %}
                union all
            {% endif %}
        {% endfor %}

        union all

        -- Genre: Homme
        select
            partition_month,
            updated_at,
            dimension_name,
            dimension_value,
            "beneficiaire_actuel_homme" as kpi_name,
            homme_users as numerator,
            1 as denominator
        from active_metrics_final

        union all

        -- Genre: Femme
        select
            partition_month,
            updated_at,
            dimension_name,
            dimension_value,
            "beneficiaire_actuel_femme" as kpi_name,
            femme_users as numerator,
            1 as denominator
        from active_metrics_final

        union all

        -- Genre: Sans genre
        select
            partition_month,
            updated_at,
            dimension_name,
            dimension_value,
            "beneficiaire_actuel_sans_genre" as kpi_name,
            sans_genre_users as numerator,
            1 as denominator
        from active_metrics_final
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
where dimension_value is not null
