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
    AND llvap.server_time >= "2021-01-01"                   -- Dates provisoires pour gérer
    AND llvap.server_time < "2022-01-01"                    -- la période d'AB testing
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
)
SELECT
    AVG(number_booked) as average,
    MAX(number_booked) as max,
    MIN(number_booked) as min
FROM number_booked_by_user
