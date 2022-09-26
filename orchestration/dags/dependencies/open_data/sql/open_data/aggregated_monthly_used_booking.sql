WITH region_format AS (
SELECT 
    * except(user_region_name, offer_category_name),
    IF(user_department_code = "99", "Étranger", user_region_name) as user_region_name,
    CASE 
        WHEN offer_category_name = "JEU" THEN "Jeu"
        WHEN offer_category_name = "FILM" THEN "Film (DVD)"
        WHEN offer_category_name = "LIVRE" THEN "Livre"
        WHEN offer_category_name = "MUSEE" THEN "Musée"
        WHEN offer_category_name = "CINEMA" THEN "Cinéma"
        WHEN offer_category_name = "SPECTACLE" THEN "Spectacle"
        WHEN offer_category_name = "BEAUX_ARTS" THEN "Matériel de Beaux-Arts"
        WHEN offer_category_name = "INSTRUMENT" THEN "Instruments de musique"
        WHEN offer_category_name = "MUSIQUE_LIVE" THEN "Musique live (concert, festival, ...)"
        WHEN offer_category_name = "PRATIQUE_ART" THEN "Pratique des arts (peinture, sculpture, ...)"
        WHEN offer_category_name = "MUSIQUE_ENREGISTREE" THEN "Musique enregistrée (CD, Vinyle, ...)"
        WHEN offer_category_name = "CONFERENCE" THEN "Conférence"
        WHEN offer_category_name = "CARTE_JEUNES" THEN "Carte jeunes"
        WHEN offer_category_name = "MEDIA" THEN "Média"
    END as offer_category_name
FROM  `{{ bigquery_analytics_dataset }}.aggregated_daily_used_booking`
)


SELECT
    DATE_TRUNC(day, MONTH) AS month,
    free_vs_paid_for,
    user_activity,
    IF(user_region_name is null, "Non Communiqué", user_department_code) as user_department_code,
    COALESCE(user_region_name, "Non Communiqué") as user_region_name,
    deposit_type,
    offer_category_name,
    sum(cnt_bookings) as cnt_bookings,
    sum(amount_spent) AS amount_spent
FROM
    region_format
WHERE
    day < DATE_TRUNC(CURRENT_DATE, MONTH)
    AND deposit_type = "GRANT_18"
GROUP BY
    1, 2, 3, 4, 5, 6, 7