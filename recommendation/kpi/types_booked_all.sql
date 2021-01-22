-- Nombre de catégories par utilisateur réservées, depuis n'importe quel parcours, sur des offres préalablement recommandées, le module ayant été vu.

WITH scrolls AS (
    SELECT
        server_time,
	    user_id_dehumanized
	FROM `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_link_visit_action_preprocessed` llvap
	LEFT JOIN `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_visit_preprocessed` lvp
	ON lvp.idvisit = llvap.idvisit
	WHERE llvap.idaction_event_action = 4394836                 --4394836 = AllModulesSeen
	AND (idaction_url=4394835 OR idaction_url=150307)           --4394835 & 150307 = page d'accueil
), booked_offers AS (
    SELECT 
        b.userId AS user_id,
        o.id AS offer_id,
        o.type,
        b.dateCreated
    FROM `pass-culture-app-projet-test.applicative_database.booking` b
    INNER JOIN `pass-culture-app-projet-test.applicative_database.stock` s
        ON b.stockId = s.id
    INNER JOIN `pass-culture-app-projet-test.applicative_database.offer` o
        ON o.id = s.offerId
    WHERE b.dateCreated >= "2020-01-01"
    AND b.dateCreated < "2022-01-01"
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
), viewed_recommended_and_booked AS (
    SELECT 
        vro.user_id,
        vro.offer_id,
        bo.type
    FROM booked_offers bo
    INNER JOIN viewed_recommended_offers vro
        ON vro.user_id = bo.user_id AND vro.offer_id = bo.offer_id
    WHERE bo.dateCreated > CAST(vro.reco_date AS DATETIME)
    GROUP BY vro.user_id, vro.offer_id, bo.type
), number_types_booked_by_user AS (
  SELECT 
      user_id,
      COUNT(DISTINCT(type)) AS number_type_booked
  FROM viewed_recommended_and_booked
  GROUP BY user_id
)
SELECT 
    AVG(number_type_booked) as average,
    MAX(number_type_booked) as max,
    MIN(number_type_booked) as min
FROM number_types_booked_by_user