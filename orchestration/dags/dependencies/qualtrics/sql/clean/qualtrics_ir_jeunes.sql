WITH user_visits AS (
    SELECT
        user_id,
        COUNT(DISTINCT CONCAT(user_pseudo_id, session_id)) AS total_visit_last_month
    FROM `{{ bigquery_analytics_dataset }}.firebase_events` 
    WHERE DATE(user_first_touch_timestamp) >= DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)
    GROUP BY user_id
),

ir_export AS (
SELECT
 

    user_data.user_id,
    deposit.type as deposit_type,
    user_data.user_civility,
    no_cancelled_booking,
    user_data.user_department_code,
    user_data.user_region_name,
    actual_amount_spent,
    user_data.user_activity,
    user_visits.total_visit_last_month,
    geo_type,
    code_qpv,
    zrr,
    user_seniority

    FROM `{{ bigquery_analytics_dataset }}.enriched_user_data` user_data
    INNER JOIN `{{ bigquery_analytics_dataset }}.applicative_database_deposit` deposit ON user_data.user_id=deposit.userid
    INNER JOIN `{{ bigquery_analytics_dataset }}.applicative_database_user` applicative_database_user ON user_data.user_id = applicative_database_user.user_id
    LEFT JOIN `{{ bigquery_analytics_dataset }}.user_locations` user_locations ON user_locations.user_id = user_data.user_id
    LEFT JOIN `{{ bigquery_analytics_dataset }}.rural_city_type_data`  rural_city_type_data ON user_locations.city_code = rural_city_type_data.geo_code
    LEFT JOIN user_visits ON user_data.user_id=user_visits.user_id
    WHERE 
        user_data.user_id is not null
    AND deposit.type in ("GRANT_15_17", "GRANT_18")
    AND user_is_current_beneficiary is true
    AND user_data.user_is_active is true
    and applicative_database_user.user_has_enabled_marketing_email is true
    
),

grant_15_17 as (
    SELECT 
        *

    FROM ir_export
    WHERE deposit_type = "GRANT_15_17"
    ORDER BY rand()
    LIMIT {{ params.volume }}
),

grant_18 as (
    SELECT 
        *

    FROM ir_export
    WHERE deposit_type = "GRANT_18"
    ORDER BY rand()
    LIMIT {{ params.volume }}
)



SELECT  
    DATE("{{ current_month(ds) }}") as calculation_month,
    * 
FROM grant_18
UNION ALL 
SELECT
    DATE("{{ current_month(ds) }}") as calculation_month,
    * 
FROM grant_15_17