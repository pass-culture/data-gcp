with
    dates as (
        select month as month
        from
            unnest(
                generate_date_array('2020-01-01', current_date(), interval 1 month)
            ) month
    ),

    diversification as (
        select
            div.user_id,
            "{{ params.group_type }}" as dimension_name,
            {% if params.group_type == "NAT" %} 'NAT'
            {% else %} rd.{{ params.group_type_name }}
            {% endif %} as dimension_value,
            user.current_deposit_type as user_type,
            date_trunc(user.first_deposit_creation_date, month) as month_deposit,
            date_trunc(deposit.deposit_expiration_date, month) as month_expiration,
            sum(delta_diversification) as diversification_indicateur
        from `{{ bigquery_analytics_dataset }}.diversification_booking` as div
        left join
            `{{ bigquery_analytics_dataset }}.global_user_beneficiary` as user
            on div.user_id = user.user_id
        left join
            `{{ bigquery_analytics_dataset }}.region_department` as rd
            on user.user_department_code = rd.num_dep
        left join
            `{{ bigquery_analytics_dataset }}.global_deposit` as deposit
            on user.user_id = deposit.user_id
        join
            `{{ bigquery_analytics_dataset }}.global_booking` as booking
            on booking.booking_id = div.booking_id
        where
            not booking_is_cancelled
            and date_diff(
                user.first_deposit_creation_date, booking.booking_created_at, day
            )
            <= 365  -- Les réservations de la première annee
        group by 1, 2, 3, 4, 5, 6
    )

select distinct
    dates.month,
    dimension_name,
    dimension_value,
    user_type,
    "diversification_median" as indicator,
    percentile_disc(diversification_indicateur, 0.5) over (
        partition by dates.month
    ) as numerator,
    1 as denominator
from dates
left join
    diversification
    on dates.month >= diversification.month_deposit
    and dates.month <= diversification.month_expiration
    and date_diff(dates.month, diversification.month_deposit, day) >= 365  -- Uniquement les utilisateurs ayant plus d'un an d'ancienneté
