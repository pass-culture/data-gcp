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
{#
    Beneficiary counts are blurred with the cell key method (see
    statistical_confidentiality doc): every count gets the cell key of the
    beneficiaries it counts, and is perturbed below NAT so that no published
    count lies between 1 and 4.
#}
{% set active_measures = {
    "total_users": "true",
    "qpv_users": "user_is_in_qpv",
    "rural_users": "user_macro_density_label = 'rural'",
    "non_scolarise_users": "not user_is_in_education",
    "homme_users": "user_civility = 'M.'",
    "femme_users": "user_civility = 'Mme.'",
    "sans_genre_users": "user_civility is null",
} %}
{% for activity in activity_list %}
    {% do active_measures.update(
        {
            activity.value
            ~ "_users": 'user_activity = "'
            ~ activity.activity
            ~ '"'
        }
    ) %}
{% endfor %}

-- Optimized version: Single-pass aggregation to avoid memory issues from 68+ UNION
-- ALL statements
with
    last_day_of_month as (
        select
            date_trunc(deposit_active_date, month) as partition_month,
            timestamp("{{ ts() }}") as updated_at,
            max(deposit_active_date) as last_active_date
        from {{ ref("int_global__daily_deposit") }}
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
        from {{ ref("int_global__daily_deposit") }} as uua
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
            eud.user_municipality_code as com_dim,
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
            eud.user_municipality_code as com_dim
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
                ) as {{ activity.value }}_users,
            {% endfor %}
            {% for measure, condition in active_measures.items() %}
                coalesce(
                    mod(
                        abs(
                            sum(
                                distinct case
                                    when {{ condition }}
                                    then {{ record_key("user_id") }}
                                end
                            )
                        ),
                        256
                    ),
                    0
                ) as {{ measure }}_cell_key
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
            count(distinct user_id) as total_users,
            mod(
                abs(sum(distinct {{ record_key("user_id") }})), 256
            ) as total_users_cell_key
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
            {% for activity in activity_list %} {{ activity.value }}_users, {% endfor %}
            {% for measure in active_measures %}
                {{ measure }}_cell_key{% if not loop.last %},{% endif %}
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
                sum({{ activity.value }}_users) as {{ activity.value }}_users,
            {% endfor %}
            -- the cell key of a union of beneficiaries is the sum of their keys
            {% for measure in active_measures %}
                mod(sum({{ measure }}_cell_key), 256) as {{ measure }}_cell_key
                {% if not loop.last %},{% endif %}
            {% endfor %}
        from active_metrics_unpivoted
        group by partition_month, updated_at, dimension_name, dimension_value
    ),

    -- UNPIVOT dimensions for total users
    total_metrics_unpivoted as (
        select
            partition_month,
            updated_at,
            dimension_name,
            dimension_value,
            total_users,
            total_users_cell_key
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
            sum(total_users) as total_users,
            mod(sum(total_users_cell_key), 256) as total_users_cell_key
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
            1 as denominator,
            total_users_cell_key as numerator_cell_key,
            cast(null as int64) as denominator_cell_key
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
            1 as denominator,
            total_users_cell_key as numerator_cell_key,
            cast(null as int64) as denominator_cell_key
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
            total_users as denominator,
            qpv_users_cell_key as numerator_cell_key,
            total_users_cell_key as denominator_cell_key
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
            total_users as denominator,
            rural_users_cell_key as numerator_cell_key,
            total_users_cell_key as denominator_cell_key
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
            total_users as denominator,
            non_scolarise_users_cell_key as numerator_cell_key,
            total_users_cell_key as denominator_cell_key
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
                total_users as denominator,
                {{ activity.value }}_users_cell_key as numerator_cell_key,
                total_users_cell_key as denominator_cell_key
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
            1 as denominator,
            homme_users_cell_key as numerator_cell_key,
            cast(null as int64) as denominator_cell_key
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
            1 as denominator,
            femme_users_cell_key as numerator_cell_key,
            cast(null as int64) as denominator_cell_key
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
            1 as denominator,
            sans_genre_users_cell_key as numerator_cell_key,
            cast(null as int64) as denominator_cell_key
        from active_metrics_final
    ),
    perturbed_metrics as (
        select
            unified_metrics.partition_month,
            unified_metrics.updated_at,
            unified_metrics.dimension_name,
            unified_metrics.dimension_value,
            unified_metrics.kpi_name,
            if(
                unified_metrics.dimension_name = 'NAT',
                unified_metrics.numerator,
                greatest(
                    unified_metrics.numerator + coalesce(pt_num.perturbation, 0), 0
                )
            ) as numerator,
            if(
                unified_metrics.dimension_name = 'NAT'
                or unified_metrics.denominator_cell_key is null,
                unified_metrics.denominator,
                greatest(
                    unified_metrics.denominator + coalesce(pt_den.perturbation, 0), 0
                )
            ) as denominator,
            unified_metrics.denominator_cell_key is not null as is_share
        from unified_metrics
        left join
            {{ ref("perturbation_table__individual") }} as pt_num
            on unified_metrics.numerator between pt_num.count_min and pt_num.count_max
            and unified_metrics.numerator_cell_key
            between pt_num.cell_key_min and pt_num.cell_key_max
        left join
            {{ ref("perturbation_table__individual") }} as pt_den
            on unified_metrics.denominator between pt_den.count_min and pt_den.count_max
            and unified_metrics.denominator_cell_key
            between pt_den.cell_key_min and pt_den.cell_key_max
        where unified_metrics.dimension_value is not null
    )

-- Shares count a sub-cohort of the denominator: perturbations are independent, so
-- the perturbed numerator is capped at the perturbed denominator (shares <= 100%)
select
    partition_month,
    updated_at,
    dimension_name,
    dimension_value,
    kpi_name,
    coalesce(if(is_share, least(numerator, denominator), numerator), 0) as numerator,
    coalesce(denominator, 0) as denominator,
    coalesce(
        safe_divide(
            if(is_share, least(numerator, denominator), numerator), denominator
        ),
        0
    ) as kpi
from perturbed_metrics
