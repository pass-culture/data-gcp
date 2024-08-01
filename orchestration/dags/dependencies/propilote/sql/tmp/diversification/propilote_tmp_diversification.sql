WITH dates AS (
    select 
        month as month
    from unnest(generate_date_array('2020-01-01', current_date(), interval 1 month)) month
),

diversification as ( 
    SELECT 
        div.user_id
        , "{{ params.group_type }}" as dimension_name
        , 
        {% if params.group_type == 'NAT' %}
            'NAT'
        {% else %}
            rd.{{ params.group_type_name }}
        {% endif %} as dimension_value
        , user.user_current_deposit_type as user_type
        , DATE_TRUNC(user.user_deposit_creation_date, MONTH) AS month_deposit
        , DATE_TRUNC(deposit.deposit_expiration_date, MONTH) AS month_expiration
        , SUM(delta_diversification) as diversification_indicateur
    FROM `{{ bigquery_analytics_dataset }}.diversification_booking` as div
    LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_user_data` as user
        ON div.user_id=user.user_id
    LEFT JOIN `{{ bigquery_analytics_dataset }}.region_department` as rd
        on  user.user_department_code = rd.num_dep 
    LEFT JOIN `{{ bigquery_analytics_dataset }}.global_deposit` as deposit
        ON user.user_id = deposit.user_id 
    JOIN `{{ bigquery_analytics_dataset }}.global_booking` as booking
        ON booking.booking_id = div.booking_id
    WHERE NOT booking_is_cancelled 
    AND DATE_DIFF(user.user_deposit_creation_date, booking.booking_created_at, DAY) <= 365 -- Les réservations de la première annee
    GROUP BY 
        1, 2, 3, 4, 5, 6
)

SELECT DISTINCT
    dates.month
    , dimension_name
    , dimension_value
    , user_type
    , "diversification_median" as indicator
    , PERCENTILE_DISC(diversification_indicateur, 0.5) OVER(PARTITION BY dates.month) AS numerator
    , 1 as denominator
FROM dates
LEFT JOIN diversification
    ON dates.month >= diversification.month_deposit 
    AND dates.month <= diversification.month_expiration
    AND DATE_DIFF(dates.month, diversification.month_deposit, DAY) >= 365 -- Uniquement les utilisateurs ayant plus d'un an d'ancienneté