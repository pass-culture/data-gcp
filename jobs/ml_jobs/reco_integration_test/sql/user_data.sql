-- get_user_data.sql
WITH user_sample AS (
    SELECT DISTINCT fe.user_id
    FROM `passculture-data-prod.analytics_prod.native_event` fe
    WHERE event_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)
    AND event_name = 'ConsultOffer'
    AND user_id IS NOT NULL
    LIMIT 1000
),
users_clics AS (
    SELECT us.user_id, CAST(fe.offer_id AS STRING) AS offer_id
    FROM user_sample us
    JOIN `passculture-data-prod.analytics_prod.native_event` fe
        ON fe.user_id = us.user_id
    WHERE event_date > DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)
    AND event_name = 'ConsultOffer'
),
base_booking AS (
    SELECT eud.user_id, CAST(ebd.offer_id AS STRING) AS offer_id
    FROM (SELECT DISTINCT user_id FROM user_sample) eud
    JOIN `passculture-data-prod.analytics_prod.global_booking` ebd
        ON ebd.user_id = eud.user_id
),
base_interaction AS (
    SELECT *, "booking" AS event_name
    FROM base_booking
    UNION ALL
    SELECT *, "clics" AS event_name
    FROM users_clics
),
enriched_interaction AS (
    SELECT
        bi.user_id, eom.offer_subcategory_id,
        eom.offer_type_label, eom.offer_sub_type_label,
        event_name, COUNT(*) AS subcat_count
    FROM base_interaction bi
    JOIN `passculture-data-prod.analytics_prod.global_offer_metadata` eom
        ON eom.offer_id = bi.offer_id
    WHERE eom.offer_subcategory_id IN ("LIVRE_PAPIER", "SUPPORT_PHYSIQUE_MUSIQUE_CD", "SEANCE_CINE")
    GROUP BY bi.user_id, offer_subcategory_id, eom.offer_type_label, eom.offer_sub_type_label, event_name
),
user_subcategory_count AS (
    SELECT user_id, offer_subcategory_id, SUM(subcat_count) AS total_count
    FROM enriched_interaction
    GROUP BY user_id, offer_subcategory_id
),
user_total_count AS (
    SELECT user_id, SUM(total_count) AS total_bookings
    FROM user_subcategory_count
    GROUP BY user_id
),
user_top_subcategory AS (
    SELECT
        us.user_id, us.offer_subcategory_id, us.total_count,
        ut.total_bookings, (us.total_count / ut.total_bookings) AS subcategory_ratio
    FROM user_subcategory_count us
    JOIN user_total_count ut ON us.user_id = ut.user_id
    WHERE (us.total_count / ut.total_bookings) >= 0.5
    AND us.offer_subcategory_id IN ("LIVRE_PAPIER", "SUPPORT_PHYSIQUE_MUSIQUE_CD", "SEANCE_CINE")
),
ranked_users AS (
    SELECT
        uts.user_id, uts.offer_subcategory_id, uts.total_count,
        uts.subcategory_ratio,
        ROW_NUMBER() OVER (PARTITION BY uts.offer_subcategory_id ORDER BY uts.total_count DESC) AS rank
    FROM user_top_subcategory uts
)
SELECT
    ru.user_id, ru.offer_subcategory_id, ru.total_count, ru.subcategory_ratio
FROM ranked_users ru
WHERE ru.rank <= 1000
ORDER BY ru.offer_subcategory_id, ru.total_count DESC;
