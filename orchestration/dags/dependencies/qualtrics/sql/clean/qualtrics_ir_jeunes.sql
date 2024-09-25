WITH user_visits AS (
    SELECT
        user_id,
        COUNT(DISTINCT CONCAT(user_pseudo_id, session_id)) AS total_visit_last_month
    FROM `{{ bigquery_int_firebase_dataset }}.native_event`
    WHERE DATE(event_date) >= DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)
    GROUP BY user_id
),

previous_export AS (
    SELECT
        DISTINCT user_id
    FROM  `{{ bigquery_clean_dataset }}.qualtrics_ir_jeunes`
    WHERE calculation_month >= DATE_SUB(DATE("{{ current_month(ds) }}"), INTERVAL 6 MONTH)


),

answers AS (
    SELECT distinct user_id
    FROM `{{ bigquery_raw_dataset }}.qualtrics_answers`
),

ir_export AS (
    SELECT
        user_data.user_id,
        user_data.current_deposit_type as deposit_type,
        user_data.user_civility,
        user_data.total_non_cancelled_individual_bookings as no_cancelled_booking,
        user_data.user_region_name,
        user_data.total_actual_amount_spent as actual_amount_spent,
        user_data.user_activity,
        user_visits.total_visit_last_month,
        -- TODO rename field in qualtrics
        user_location.user_rural_city_type as geo_type,
        user_location.code_qpv as code_qpv,
        user_location.zrr_level as zrr,
        user_data.user_seniority

        FROM `{{ bigquery_analytics_dataset }}.global_user` user_data
        LEFT JOIN `{{ bigquery_int_geo_dataset }}.user_location` user_location ON user_location.user_id = user_data.user_id
        LEFT JOIN `{{ bigquery_raw_dataset }}.qualtrics_opt_out_users` opt_out on opt_out.ext_ref = user_data.user_id
        LEFT JOIN user_visits ON user_data.user_id = user_visits.user_id
        LEFT JOIN answers ON user_data.user_id = answers.user_id
        WHERE
            user_data.user_id is not null
        AND user_data.current_deposit_type in ("GRANT_15_17", "GRANT_18")
        AND user_is_current_beneficiary is true
        AND user_data.user_is_active is true
        AND user_data.user_has_enabled_marketing_email is true
        AND opt_out.contact_id IS NULL
        AND answers.user_id IS NULL
),

grant_15_17 as (
    SELECT
        ir.*
    FROM ir_export ir
    LEFT JOIN previous_export pe
    ON pe.user_id = ir.user_id
    WHERE ir.deposit_type = "GRANT_15_7"
    AND pe.user_id IS NULL
    ORDER BY rand()
    LIMIT {{ params.volume }}
),

grant_18 as (
    SELECT
        ir.*
    FROM ir_export ir
    LEFT JOIN previous_export pe
    ON pe.user_id = ir.user_id
    WHERE deposit_type = "GRANT_18"
    AND pe.user_id IS NULL
    ORDER BY rand()
    LIMIT {{ params.volume }}
)

SELECT
    DATE("{{ current_month(ds) }}") as calculation_month,
    CURRENT_DATE as export_date,
    *
FROM grant_18
UNION ALL
SELECT
    DATE("{{ current_month(ds) }}") as calculation_month,
    CURRENT_DATE as export_date,
    *
FROM grant_15_17
