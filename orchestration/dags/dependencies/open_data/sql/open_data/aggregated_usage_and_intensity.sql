WITH aggregated_monthly_user_used_booking_activity AS (
    SELECT
        * except(user_region_name),
     IF(user_department_code = "99", null, user_region_name) as user_region_name
    FROM
        `{{ bigquery_analytics_dataset }}.aggregated_monthly_user_used_booking_activity`
    WHERE
        -- today
        active_month < DATE_TRUNC(DATE("{{ add_days(ds, 1) }}"), MONTH)
        AND deposit_type = 'GRANT_18'
)

SELECT
    DATE("{{ current_month(add_days(ds, 1)) }}") as calculation_month,
    IF(user_region_name is null, "Non Renseigné", user_department_code) as user_department_code,
    COALESCE(user_region_name, "Non Renseigné") as user_region_name,
    -- Nombre d'inscrits ayant fait au moins une réservation validée dans les 3 premiers mois après l’obtention de son crédit, parmi les jeunes inscrits il y a entre 3 et 6 mois
    COUNT(
        DISTINCT CASE
            WHEN months_since_deposit_created = 3
            AND cumulative_cnt_used_bookings >= 1
            AND seniority_months BETWEEN 3
            AND 6 THEN user_id
            ELSE NULL
        END
    ) AS utils_kpi1,
    -- Nombre d'inscrits ayant fait au moins une réservation validée dans les 12 premiers mois après l’obtention de son crédit , parmi les jeunes inscrits il y a entre 12 et 15 mois
    COUNT(
        DISTINCT CASE
            WHEN months_since_deposit_created = 12
            AND cumulative_cnt_used_bookings >= 1
            AND seniority_months BETWEEN 12
            AND 15 THEN user_id
            ELSE NULL
        END
    ) AS utils_kpi2,
    -- Nombre d'inscrits ayant fait au moins une réservation validée dans les 24 premiers mois après l’obtention de son crédit , parmi les jeunes inscrits il y a entre 24 et 27 mois
    COUNT(
        DISTINCT CASE
            WHEN months_since_deposit_created = 24
            AND cumulative_cnt_used_bookings >= 1
            AND seniority_months BETWEEN 24
            AND 27 THEN user_id
            ELSE NULL
        END
    ) AS utils_kpi3,
    -- Nombre d’utilisateurs ayant effectué 3 réservations dans les 3 premiers mois après l’obtention de son crédit, parmi les jeunes inscrits il y a entre 3 et 6 mois
    COUNT(
        DISTINCT CASE
            WHEN months_since_deposit_created = 3
            AND cumulative_cnt_used_bookings >= 3
            AND seniority_months BETWEEN 3
            AND 6 THEN user_id
            ELSE NULL
        END
    ) AS utils_kpi4,
    -- Nombre d’utilisateurs ayant effectué 3 réservations dans les 12 premiers mois après l’obtention de son crédit, parmi les jeunes inscrits il y a entre 12 et 15 mois
    COUNT(
        DISTINCT CASE
            WHEN months_since_deposit_created = 12
            AND cumulative_cnt_used_bookings >= 3
            AND seniority_months BETWEEN 12
            AND 15 THEN user_id
            ELSE NULL
        END
    ) AS utils_kpi5,
    -- Nombre d’utilisateurs ayant effectué 3 réservations dans les 24 premiers mois après l’obtention de son crédit, parmi les jeunes inscrits il y a entre 24 et 27 mois
    COUNT(
        DISTINCT CASE
            WHEN months_since_deposit_created = 24
            AND cumulative_cnt_used_bookings >= 3
            AND seniority_months BETWEEN 24
            AND 27 THEN user_id
            ELSE NULL
        END
    ) AS utils_kpi6,
    -- Consommation total du crédit pass dans les 3 premiers mois après l’obtention de son crédit, parmi les jeunes inscrits il y a entre 3 et 6 mois
    SUM(
        CASE
            WHEN months_since_deposit_created = 3
            AND seniority_months BETWEEN 3
            AND 6 THEN cumulative_amount_spent
            ELSE NULL
        END
    ) AS intens_sum_kpi1,
    -- Consommation total du crédit pass dans les 12 premiers mois après l’obtention de son crédit, parmi les jeunes inscrits il y a entre 12 et 15 mois
    SUM(
        CASE
            WHEN months_since_deposit_created = 12
            AND seniority_months BETWEEN 12
            AND 15 THEN cumulative_amount_spent
            ELSE NULL
        END
    ) AS intens_sum_kpi2,
    -- Consommation total du crédit pass dans les 24 premiers mois après l’obtention de son crédit, parmi les jeunes inscrits il y a entre 24 et 27 mois
    SUM(
        CASE
            WHEN months_since_deposit_created = 24
            AND seniority_months BETWEEN 24
            AND 27 THEN cumulative_amount_spent
            ELSE NULL
        END
    ) AS intens_sum_kpi3,
    -- Nombre total de jeunes inscrits il y a entre 3 et 6 mois
    COUNT(
        DISTINCT CASE
            WHEN months_since_deposit_created = 3
            AND seniority_months BETWEEN 3
            AND 6 THEN user_id
            ELSE NULL
        END
    ) AS intens_user_kpi1,
    -- Nombre total jeunes inscrits il y a entre 12 et 15 mois
    COUNT(
        DISTINCT CASE
            WHEN months_since_deposit_created = 12
            AND seniority_months BETWEEN 12
            AND 15 THEN user_id
            ELSE NULL
        END
    ) AS intens_user_kpi2,
    --  Nombre total jeunes inscrits il y a entre 24 et 27 mois
    COUNT(
        DISTINCT CASE
            WHEN months_since_deposit_created = 24
            AND seniority_months BETWEEN 24
            AND 27 THEN user_id
            ELSE NULL
        END
    ) AS intens_user_kpi3
FROM
    aggregated_monthly_user_used_booking_activity
GROUP BY
    1,
    2,
    3