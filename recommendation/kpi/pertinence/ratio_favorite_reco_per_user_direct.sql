-- Ratio nombre de mises en favoris / reco par utilisateur sur offres recommandées directes

WITH scrolls AS (
    SELECT server_time, user_id_dehumanized
	FROM `passculture-data-prod.clean_prod.log_link_visit_action_preprocessed` llvap
	JOIN `passculture-data-prod.clean_prod.matomo_visits` lvp
	ON lvp.idvisit = llvap.idvisit
	WHERE llvap.idaction_event_action = 4394836                 --4394836 = AllModulesSeen
	AND (idaction_url=4394835 OR idaction_url=150307)           --4394835 & 150307 = page d'accueil
    AND llvap.server_time >= PARSE_TIMESTAMP('%Y%m%d',@DS_START_DATE)     -- Dates à définir sur la dashboard
    AND llvap.server_time < PARSE_TIMESTAMP('%Y%m%d',@DS_END_DATE)        -- pour gérer la période d'AB testing
), favorite_offers AS (
    SELECT
        distinct(lap.tracker_data.dehumanize_offer_id) as offer_id,
        lvp.user_id_dehumanized,
        llvap.server_time
    FROM `passculture-data-prod.clean_prod.log_action_preprocessed` lap
    JOIN `passculture-data-prod.clean_prod.log_link_visit_action_preprocessed` llvap
        ON lap.raw_data.idaction = llvap.idaction_name
    JOIN `passculture-data-prod.clean_prod.matomo_visits` lvp
        ON lvp.idvisit = llvap.idvisit
    WHERE idaction_event_action = 6957092                              -- 6957092: AddFavorite_FromHomepage
    AND llvap.server_time >= PARSE_TIMESTAMP('%Y%m%d',@DS_START_DATE)     -- Dates à définir sur la dashboard
    AND llvap.server_time < PARSE_TIMESTAMP('%Y%m%d',@DS_END_DATE)        -- pour gérer la période d'AB testing
    AND lap.tracker_data.module_name = 'King Kendrick'           -- A MODIFIER
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
), viewed_recommended_and_favorite AS (
    SELECT
    vro.user_id,
    vro.offer_id
    FROM favorite_offers fo
    INNER JOIN viewed_recommended_offers vro
    ON vro.user_id = fo.user_id_dehumanized AND vro.offer_id = fo.offer_id
    WHERE fo.server_time > vro.reco_date
    GROUP BY vro.user_id, vro.offer_id
), number_favorite_by_user AS (
  SELECT
      user_id,
      COUNT(*) AS number_favorite
  FROM viewed_recommended_and_favorite
  GROUP BY user_id
), number_recommendations_by_user AS (
    SELECT
        vro.user_id,
        COUNT(*) AS number_reco
    FROM viewed_recommended_offers vro
    GROUP BY user_id
), ratio_favorite_reco_by_user AS (
  SELECT
      ncbu.user_id,
      ncbu.number_favorite / nrbu.number_reco AS ratio_favorite_reco
  FROM number_recommendations_by_user nrbu
  INNER JOIN number_favorite_by_user ncbu
  ON ncbu.user_id = nrbu.user_id
)
SELECT
    AVG(ratio_favorite_reco) as average,
    MAX(ratio_favorite_reco) as max,
    MIN(ratio_favorite_reco) as min
FROM ratio_favorite_reco_by_user

