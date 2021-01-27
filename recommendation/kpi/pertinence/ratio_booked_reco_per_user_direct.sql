-- Ratio nombre de bookings / reco par utilisateur sur offres recommandées directes

WITH scrolls AS (
    SELECT
        server_time,
	    user_id_dehumanized
	FROM `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_link_visit_action_preprocessed` llvap
	INNER JOIN `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_visit_preprocessed` lvp
	    ON lvp.idvisit = llvap.idvisit
	WHERE llvap.idaction_event_action = 4394836                 --4394836 = AllModulesSeen
	AND (idaction_url=4394835 OR idaction_url=150307)           --4394835 & 150307 = page d'accueil
    AND llvap.server_time >= PARSE_TIMESTAMP('%Y%m%d',@DS_START_DATE)     -- Dates à définir sur la dashboard
    AND llvap.server_time < PARSE_TIMESTAMP('%Y%m%d',@DS_END_DATE)        -- pour gérer la période d'AB testing
), booked_offers_from_reco AS (
    SELECT
        lvp.user_id_dehumanized AS user_id,
        lap.tracker_data.dehumanize_offer_id AS offer_id,
        llvap.server_time
    FROM `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_link_visit_action_preprocessed` AS llvap
    INNER JOIN `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_visit_preprocessed` AS lvp
        ON lvp.idvisit = llvap.idvisit
    INNER JOIN `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_action_preprocessed` AS lap
        ON llvap.idaction_name = lap.raw_data.idaction
    WHERE idaction_event_action = 6957147                   -- 6957147: BookOfferClick_FromHomepage
    AND lap.tracker_data.module_name = 'undefined'          -- A MODIFIER
    AND llvap.server_time >= PARSE_TIMESTAMP('%Y%m%d',@DS_START_DATE)     -- Dates à définir sur la dashboard
    AND llvap.server_time < PARSE_TIMESTAMP('%Y%m%d',@DS_END_DATE)        -- pour gérer la période d'AB testing
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
    vro.offer_id
    FROM booked_offers_from_reco bofr
    INNER JOIN viewed_recommended_offers vro
        ON vro.user_id = bofr.user_id AND vro.offer_id = bofr.offer_id
    WHERE bofr.server_time > vro.reco_date
    GROUP BY vro.user_id, vro.offer_id
), number_booked_by_user AS (
    SELECT
    user_id,
    count(*) AS number_booked
    FROM viewed_recommended_and_booked
    GROUP BY user_id
), number_recommendations_by_user AS (
    SELECT
        vro.user_id,
        COUNT(*) AS number_reco
    FROM viewed_recommended_offers vro
    GROUP BY user_id
), ratio_booked_reco_by_user AS (
    SELECT
        nbbu.user_id,
        nbbu.number_booked / nrbu.number_reco AS ratio_booked_reco
    FROM number_recommendations_by_user nrbu
    INNER JOIN number_booked_by_user nbbu
    ON nbbu.user_id = nrbu.user_id
)
SELECT
    AVG(ratio_booked_reco) as average,
    MAX(ratio_booked_reco) as max,
    MIN(ratio_booked_reco) as min
FROM ratio_booked_reco_by_user
