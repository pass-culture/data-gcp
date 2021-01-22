-- Nombre de catégories par utilisateur cliquées, depuis n'importe quel parcours, sur des offres préalablement recommandées, le module ayant été vu.

WITH scrolls AS (
    SELECT
        server_time,
	    user_id_dehumanized
	FROM `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_link_visit_action_preprocessed` llvap
	LEFT JOIN `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_visit_preprocessed` lvp
	ON lvp.idvisit = llvap.idvisit
	WHERE llvap.idaction_event_action = 4394836                 --4394836 = AllModulesSeen
	AND (idaction_url=4394835 OR idaction_url=150307)           --4394835 & 150307 = page d'accueil
), clicked_offers AS (
    SELECT
        distinct(lap.url_data.dehumanize_offer_id) as offer_id,
        lvp.user_id_dehumanized,
        llvap.server_time
    FROM `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_action_preprocessed` lap
    JOIN `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_link_visit_action_preprocessed` llvap
        ON lap.raw_data.idaction = llvap.idaction_url
    JOIN `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_visit_preprocessed` lvp
        ON lvp.idvisit = llvap.idvisit
    WHERE
        lap.url_data.dehumanize_offer_id is not null
        AND llvap.server_time >= "2021-01-01"
        AND llvap.server_time < "2022-01-01"
), recommended_offers AS (
	SELECT
        userId,
        offerId,
        date 
	FROM `pass-culture-app-projet-test.algo_reco_kpi_data.past_recommended_offers`
), viewed_recommended_offers AS (
	SELECT
        *
    FROM (
        SELECT
            CAST(ro.userId AS STRING) AS user_id,
            CAST(ro.offerId AS STRING) AS offer_id,
            ro.date AS reco_date,
            s.server_time AS scroll_time,
            RANK() OVER (PARTITION BY ro.userId, ro.date ORDER BY TIMESTAMP_DIFF(ro.date, s.server_time, SECOND)) AS time_rank
	    FROM recommended_offers ro
		LEFT JOIN scrolls s
	    ON CAST(ro.userId AS STRING) = s.user_id_dehumanized
	    WHERE s.server_time >= ro.date
    )
    WHERE time_rank = 1
), viewed_recommended_and_clicked AS (
    SELECT 
    vro.user_id,
    vro.offer_id,
    o.type
    FROM clicked_offers co
    INNER JOIN viewed_recommended_offers vro
        ON vro.user_id = co.user_id_dehumanized AND vro.offer_id = co.offer_id
    INNER JOIN `pass-culture-app-projet-test.applicative_database.offer` o
        ON o.id = co.offer_id
    WHERE co.server_time > vro.reco_date
    GROUP BY vro.user_id, vro.offer_id, o.type
), number_types_clicked_by_user AS (
  SELECT 
      user_id,
      COUNT(DISTINCT(type)) AS number_type_clicked
  FROM viewed_recommended_and_clicked
  GROUP BY user_id
)
SELECT 
    AVG(number_type_clicked) as average,
    MAX(number_type_clicked) as max,
    MIN(number_type_clicked) as min
FROM number_types_clicked_by_user