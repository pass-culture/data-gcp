-- noqa: disable=all
with
    dates as (
        select month as month
        from
            unnest(
                generate_date_array('2020-01-01', current_date(), interval 1 month)
            ) month
    ),

    infos_users as (
        select
            deposit.user_id,
            date_trunc(deposit_creation_date, month) as date_deposit,
            date_trunc(deposit_expiration_date, month) as date_expiration,
            user.first_individual_booking_date as first_booking_date,
            user.user_department_code,
            user.user_region_name,
            rd.academy_name,
            case
                when deposit.deposit_type = "GRANT_17_18" and deposit.user_age <= 17
                then "GRANT_15_17"
                when deposit.deposit_type = "GRANT_17_18" and deposit.user_age >= 18
                then "GRANT_18"
                else deposit.deposit_type
            end as deposit_type
        from `{{ bigquery_analytics_dataset }}.global_deposit` as deposit
        join
            `{{ bigquery_analytics_dataset }}.global_user` as user
            on deposit.user_id = user.user_id
        left join
            `{{ bigquery_seed_dataset }}.region_department` as rd
            on user.user_department_code = rd.num_dep
    )

select
    month,  -- tous les month
    "{{ params.group_type }}" as dimension_name,
    {% if params.group_type == "NAT" %} 'NAT'
    {% else %} {{ params.group_type_name }}
    {% endif %} as dimension_value,
    deposit_type as user_type,
    "taux_activation" as indicator,
    count(
        distinct
        case
            when
                date_diff(first_booking_date, date_deposit, day)
                <= {{ params.months_threshold }}
            then user_id
            else null
        end
    ) as numerator,  -- ceux qui sont bénéficiaires actuels et qui ont fait 1 résa dans les 3 month après inscription
    count(distinct user_id) as denominator  -- ceux qui sont bénéficiaires actuels
from dates
left join
    infos_users
    on dates.month >= infos_users.date_deposit  -- ici pour prendre uniquement les bénéficiaires actuels
    and dates.month <= infos_users.date_expiration  -- idem
    and date_diff(current_date, date_deposit, day) >= {{ params.months_threshold }}  -- Base = uniquement les jeunes inscrits depuis +3 month
group by 1, 2, 3, 4, 5
