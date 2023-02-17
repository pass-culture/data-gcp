WITH table_format AS (
    SELECT 
        DATE(booking_data.booking_used_date) AS day,
        CASE
            WHEN booking_data.digital_goods THEN "Produits numériques"
            ELSE "Biens et services"
        END AS product_type,
        CASE
            WHEN booking_data.booking_amount = 0 THEN "Gratuit"
            ELSE "Payant"
        END AS free_vs_paid_for,
        user_data.user_department_code,
        IF(user_data.user_department_code = "99", null, user_region_name) as user_region_name,
        CASE 
            WHEN user_data.user_civility = 'M.' THEN "Homme"
            WHEN user_data.user_civility = 'Mme' THEN "Femme"
            ELSE "Non Renseigné"
        END as user_civility,
        CASE 
            WHEN booking_data.offer_category_id = "JEU" THEN "Jeu"
            WHEN booking_data.offer_category_id = "FILM" THEN "Film (DVD, Streaming VOD...)"
            WHEN booking_data.offer_category_id = "LIVRE" THEN "Livre"
            WHEN booking_data.offer_category_id = "MUSEE" THEN "Musée"
            WHEN booking_data.offer_category_id = "CINEMA" THEN "Cinéma"
            WHEN booking_data.offer_category_id = "SPECTACLE" THEN "Spectacle vivant"
            WHEN booking_data.offer_category_id = "BEAUX_ARTS" THEN "Matériel de Beaux-Arts"
            WHEN booking_data.offer_category_id = "INSTRUMENT" THEN "Instruments de musique"
            WHEN booking_data.offer_category_id = "MUSIQUE_LIVE" THEN "Musique live (concert, festival, ...)"
            WHEN booking_data.offer_category_id = "PRATIQUE_ART" THEN "Pratique des arts (peinture, sculpture, ...)"
            WHEN booking_data.offer_category_id = "MUSIQUE_ENREGISTREE" THEN "Musique enregistrée (CD, Vinyle, Streaming...)"
            WHEN booking_data.offer_category_id = "CONFERENCE" THEN "Conférence"
            WHEN booking_data.offer_category_id = "CARTE_JEUNES" THEN "Carte jeune"
            WHEN booking_data.offer_category_id = "MEDIA" THEN "Presse"
        ELSE "Autre"
        END as offer_category_name,
        booking_data.user_id,
        booking_data.deposit_type,
        booking_data.booking_id,
        CASE
            WHEN booking_used_date IS NOT NULL THEN booking_intermediary_amount
            ELSE NULL
        END as amount_spent
    FROM
      `{{ bigquery_analytics_dataset }}.enriched_booking_data` booking_data
    JOIN `{{ bigquery_analytics_dataset }}.enriched_user_data` user_data ON booking_data.user_id = user_data.user_id
    WHERE booking_data.booking_is_used
    AND DATE(booking_data.booking_used_date) >= DATE("2019-02-01")
)


SELECT
    DATE_TRUNC(day, MONTH) AS month,
    free_vs_paid_for,
    user_civility,
    IF(user_region_name is null, "Non Renseigné", user_department_code) as user_department_code,
    COALESCE(user_region_name, "Non Renseigné") as user_region_name,
    deposit_type,
    offer_category_name,
    COUNT(DISTINCT user_id) AS cnt_users,
    count(DISTINCT booking_id) as cnt_bookings,
    SUM(amount_spent) AS amount_spent
FROM
    table_format
WHERE
    day < DATE_TRUNC(CURRENT_DATE, MONTH)
    AND deposit_type = "GRANT_18"
    AND offer_category_name != "TECHNIQUE"
GROUP BY
    1, 2, 3, 4, 5, 6, 7