with
    -- one row per user, month and department: a user active on several days of a
    -- month is counted, and keyed, once
    user_deposit as (
        select distinct
            dud.user_id,
            dud.user_department_code as department_code,
            date(date_trunc(dud.deposit_active_date, month)) as deposit_active_month,
            date_diff(dud.deposit_active_date, dud.user_birth_date, month)
            / 12.0 as user_decimal_age
        from {{ ref("int_global__daily_deposit") }} as dud
        where dud.deposit_active_date > date_sub(current_date(), interval 48 month)
    ),

    beneficiary_coverage as (
        select
            ub.deposit_active_month,
            cast(cast(ub.user_decimal_age as string) as integer) as milestone_age,
            ub.department_code,
            rd.dep_name as department_name,
            rd.region_name,
            rd.region_code,
            rd.territory_type,
            count(distinct ub.user_id) as total_beneficiaries,
            mod(sum({{ record_key("ub.user_id") }}), 256) as cell_key_beneficiaries
        from user_deposit as ub
        inner join
            {{ ref("region_department") }} as rd on ub.department_code = rd.num_dep
        where cast(ub.user_decimal_age as string) in ("16", "17", "18", "19", "20")
        group by
            ub.deposit_active_month,
            ub.user_decimal_age,
            ub.department_code,
            rd.dep_name,
            rd.region_name,
            rd.region_code,
            rd.territory_type
    ),

    -- every month of the period for every department and age, so that the rolling
    -- windows below span 12 calendar months even when a month has no beneficiary
    month_spine as (
        select
            cells.region_name,
            cells.region_code,
            cells.territory_type,
            cells.department_name,
            cells.department_code,
            cells.milestone_age,
            partition_month
        from
            (
                select distinct
                    region_name,
                    region_code,
                    territory_type,
                    department_name,
                    department_code,
                    milestone_age
                from beneficiary_coverage
            ) as cells
        cross join
            unnest(
                generate_date_array(
                    (
                        select min(deposit_active_month) as first_month
                        from beneficiary_coverage
                    ),
                    (
                        select max(deposit_active_month) as last_month
                        from beneficiary_coverage
                    ),
                    interval 1 month
                )
            ) as partition_month
    ),

    final_data as (
        select
            spine.partition_month,
            spine.region_name,
            spine.region_code,
            spine.territory_type,
            spine.department_name,
            spine.department_code,
            spine.milestone_age,
            coalesce(bc.total_beneficiaries, 0) as total_beneficiaries,
            sum(coalesce(bc.total_beneficiaries, 0)) over (
                partition by spine.milestone_age, spine.department_code
                order by spine.partition_month
                rows between 11 preceding and current row
            ) as total_beneficiaries_last_12_months,
            sum(coalesce(bc.cell_key_beneficiaries, 0)) over (
                partition by spine.milestone_age, spine.department_code
                order by spine.partition_month
                rows between 11 preceding and current row
            ) as cell_key_rolling
        from month_spine as spine
        left join
            beneficiary_coverage as bc
            on spine.partition_month = bc.deposit_active_month
            and spine.department_code = bc.department_code
            and spine.milestone_age = bc.milestone_age
    )

select
    partition_month,
    region_name,
    region_code,
    territory_type,
    department_name,
    department_code,
    milestone_age,
    total_beneficiaries as total_monthly_beneficiaries,
    total_beneficiaries_last_12_months,
    mod(cell_key_rolling, 256) as cell_key_beneficiaries
from final_data
-- months without any beneficiary over the last 12 months carry no information
where total_beneficiaries_last_12_months > 0
