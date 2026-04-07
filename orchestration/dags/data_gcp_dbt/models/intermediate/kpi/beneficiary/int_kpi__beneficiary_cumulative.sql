{% set secret_threshold_beneficiary = 5 %}

with
    last_day_of_month as (
        select
            date_trunc(deposit_active_date, month) as partition_month,
            max(deposit_active_date) as last_active_date
        from {{ ref("mrt_native__daily_user_deposit") }}
        where deposit_active_date > date("2021-01-01")
        group by date_trunc(deposit_active_date, month)
    ),

    total_users_base as (
        select
            eud.user_id,
            ldm.partition_month,
            rd.region_name,
            rd.region_code,
            rd.dep_name as department_name,
            rd.num_dep as department_code,
            -- Génération des âges : on prend tous les âges entre l'âge au 1er dépôt
            -- et l'âge actuel
            -- possible_ages as age_at_calculation,
            eud.user_is_in_qpv as is_in_qpv,
            eud.user_macro_density_label as macro_density_label,
            eud.user_density_label as micro_density_label
        from last_day_of_month as ldm
        inner join
            {{ ref("int_global__user_beneficiary") }} as eud
            on date(eud.first_deposit_creation_date) <= date(ldm.last_active_date)
            and (eud.user_is_active or eud.user_suspension_reason = "upon user request")
            and eud.current_deposit_type != "GRANT_FREE"
        -- CROSS JOIN UNNEST pour créer une ligne par âge atteint
        -- cross join
        -- unnest(
        -- generate_array(
        -- date_diff(
        -- date(eud.first_deposit_creation_date), eud.user_birth_date, year
        -- ),
        -- date_diff(date(ldm.last_active_date), eud.user_birth_date, year)
        -- )
        -- ) as possible_ages
        left join
            {{ ref("region_department") }} as rd
            on eud.user_department_code = rd.num_dep
    ),

    final_data as (
        select
            partition_month,
            region_name,
            region_code,
            department_name,
            department_code,
            -- age_at_calculation,
            is_in_qpv,
            macro_density_label,
            micro_density_label,
            count(distinct user_id) as total_cumulative_beneficiaries
        from total_users_base
        group by
            partition_month,
            region_name,
            region_code,
            department_name,
            department_code,
            -- age_at_calculation,
            is_in_qpv,
            macro_density_label,
            micro_density_label
    )

select
    partition_month,
    case
        when total_cumulative_beneficiaries <= {{ secret_threshold_beneficiary }}
        then true
        else false
    end as is_statistc_secret,
    region_name,
    region_code,
    department_name,
    department_code,
    is_in_qpv,
    macro_density_label,
    micro_density_label,
    total_cumulative_beneficiaries
from final_data
