WITH aggregated_monthly_user_used_bookings_activity AS (
    SELECT
        *
    FROM
        `{{ bigquery_analytics_dataset }}.aggregated_monthly_user_used_bookings_activity`
    WHERE months_since_deposit_created < DATE_TRUNC(CURRENT_DATE, MONTH) 
)
SELECT
 -- Nombre d'inscrits ayant fait au moins une réservation validée dans les 3 premiers mois après l’obtention de son crédit, parmi les jeunes inscrits il y a entre 3 et 6 mois
    COUNT(
        CASE
            WHEN months_since_deposit_created = 3
            AND cumulative_cnt_used_bookings >= 1
            AND seniority_months BETWEEN 3
            AND 6 THEN user_id
            ELSE NULL
        END
    ) AS utils_kpi1,
 -- Nombre d'inscrits ayant fait au moins une réservation validée dans les 12 premiers mois après l’obtention de son crédit , parmi les jeunes inscrits il y a entre 12 et 15 mois
    COUNT(
        CASE
            WHEN months_since_deposit_created = 12
            AND cumulative_cnt_used_bookings >= 1
            AND seniority_months BETWEEN 12
            AND 15 THEN user_id
            ELSE NULL
        END
    ) AS utils_kpi2,
 -- Nombre d'inscrits ayant fait au moins une réservation validée dans les 24 premiers mois après l’obtention de son crédit , parmi les jeunes inscrits il y a entre 24 et 27 mois
    COUNT(
        CASE
            WHEN months_since_deposit_created = 24
            AND cumulative_cnt_used_bookings >= 1
            AND seniority_months BETWEEN 24
            AND 27 THEN user_id
            ELSE NULL
        END
    ) AS utils_kpi3,
-- Nombre d’utilisateurs ayant effectué 3 réservations dans les 3 premiers mois après l’obtention de son crédit, parmi les jeunes inscrits il y a entre 3 et 6 mois
    COUNT(
        CASE
            WHEN months_since_deposit_created = 3
            AND cumulative_cnt_used_bookings >= 3
            AND seniority_months BETWEEN 3
            AND 6 THEN user_id
            ELSE NULL
        END
    ) AS utils_kpi4,
-- Nombre d’utilisateurs ayant effectué 3 réservations dans les 12 premiers mois après l’obtention de son crédit, parmi les jeunes inscrits il y a entre 12 et 15 mois
    COUNT(
        CASE
            WHEN months_since_deposit_created = 12
            AND cumulative_cnt_used_bookings >= 3
            AND seniority_months BETWEEN 12
            AND 15 THEN user_id
            ELSE NULL
        END
    ) AS utils_kpi5,
-- Nombre d’utilisateurs ayant effectué 3 réservations dans les 24 premiers mois après l’obtention de son crédit, parmi les jeunes inscrits il y a entre 24 et 27 mois
    COUNT(
        CASE
            WHEN months_since_deposit_created = 24
            AND cumulative_cnt_used_bookings >= 3
            AND seniority_months BETWEEN 24
            AND 27 THEN user_id
            ELSE NULL
        END
    ) AS utils_kpi6,
-- Consommation moyenne du crédit pass dans les 3 premiers mois après l’obtention de son crédit, parmi les jeunes inscrits il y a entre 3 et 6 mois
    AVG(
        CASE
            WHEN months_since_deposit_created = 3
            AND seniority_months BETWEEN 3
            AND 6 THEN user_id
            ELSE NULL
        END
    ) AS intens_kpi1,
-- Consommation moyenne du crédit pass dans les 12 premiers mois après l’obtention de son crédit, parmi les jeunes inscrits il y a entre 12 et 15 mois
    AVG(
        CASE
            WHEN months_since_deposit_created = 12
            AND seniority_months BETWEEN 12
            AND 15 THEN user_id
            ELSE NULL
        END
    ) AS intens_kpi2,
-- Consommation moyenne du crédit pass dans les 24 premiers mois après l’obtention de son crédit, parmi les jeunes inscrits il y a entre 24 et 27 mois
    AVG(
        CASE
            WHEN months_since_deposit_created = 24
            AND seniority_months BETWEEN 24
            AND 27 THEN user_id
            ELSE NULL
        END
    ) AS intens_kpi3
FROM
    aggregated_monthly_user_used_bookings_activity