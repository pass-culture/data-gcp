-- Ratio Nombre de réservations/nombre d'offres recommandées, depuis n'importe quel parcours, par utilisateur, sur des offres préalablement recommandées, le module ayant été vu.

WITH scrolls AS (
    SELECT
        server_time,
	    user_id_dehumanized
	FROM `passculture-data-prod.clean_prod.log_link_visit_action_preprocessed` llvap
	INNER JOIN `passculture-data-prod.clean_prod.matomo_visits` lvp
	    ON lvp.idvisit = llvap.idvisit
	WHERE llvap.idaction_event_action = 4394836                 --4394836 = AllModulesSeen
	AND (idaction_url=4394835 OR idaction_url=150307)           --4394835 & 150307 = page d'accueil
    AND llvap.server_time >= PARSE_TIMESTAMP('%Y%m%d',@DS_START_DATE)     -- Dates à définir sur la dashboard
    AND llvap.server_time < PARSE_TIMESTAMP('%Y%m%d',@DS_END_DATE)        -- pour gérer la période d'AB testing
), booked_offers AS (
    SELECT user_id, offer_id AS offerId, CAST(booking_creation_date AS TIMESTAMP) AS booking_date
    FROM `passculture-data-prod.analytics_prod.applicative_database_booking` b
	JOIN `passculture-data-prod.analytics_prod.applicative_database_stock` s
	ON b.stock_id = s.stock_id
--    WHERE booking_date >= "2021-01-01"
--    AND booking_date < "2022-01-01"
), recommended_offers AS (
	SELECT
        userId,
        offerId,
        date
	FROM `passculture-data-prod.raw_prod.past_recommended_offers`
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
		INNER JOIN scrolls s
	    ON CAST(ro.userId AS STRING) = s.user_id_dehumanized
	    WHERE s.server_time >= ro.date
    )
    WHERE time_rank = 1
), viewed_recommended_and_booked AS (
    SELECT
    vro.user_id,
    vro.offer_id
    FROM booked_offers bo
    INNER JOIN viewed_recommended_offers vro
        ON vro.user_id = bo.user_id AND vro.offer_id = bo.offerId
    WHERE bo.booking_date  > vro.reco_date
    GROUP BY vro.user_id, vro.offer_id
), number_booked_by_user AS (
  SELECT
      user_id,
      COUNT(*) AS number_clicked
  FROM viewed_recommended_and_booked
  GROUP BY user_id
), number_recommendations_by_user AS (
    SELECT
        vro.user_id,
        COUNT(*) AS number_reco
    FROM viewed_recommended_offers vro
    GROUP BY user_id
), ratio_clicked_reco_by_user AS (
  SELECT
      nbbu.user_id,
      nbbu.number_clicked / nrbu.number_reco AS ratio_clicked_reco
  FROM number_recommendations_by_user nrbu
  INNER JOIN number_booked_by_user nbbu
        ON nbbu.user_id = nrbu.user_id
  INNER JOIN `passculture-data-prod.raw_prod.ab_testing_20201207` ab
        ON nbbu.user_id = ab.userid
  WHERE ab.groupid = "A"
)
SELECT
    AVG(ratio_clicked_reco) as average,
    MAX(ratio_clicked_reco) as max,
    MIN(ratio_clicked_reco) as min
FROM ratio_clicked_reco_by_user
