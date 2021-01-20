/* Pertinence online queries */
-- TODO: change reco module name 'Module de reco' after MEP

-- Number of consulted offers FROM recommendation module
WITH consulted_offers AS (
SELECT * FROM `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_link_visit_action_preprocessed` AS llva
WHERE  idaction_event_action = 6956932
),
consulted_offers_FROM_reco_module AS (
SELECT * FROM consulted_offers AS co
LEFT JOIN `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_action_preprocessed` AS la
ON co.idaction_name = la.raw_data.idaction
WHERE la.tracker_data.module_name = 'Module de reco' AND (idaction_url=4394835 OR idaction_url=150307)
)
SELECT count(*) FROM consulted_offers_FROM_reco_module;

-- Number of favorite offers FROM recommendation module
WITH favorite_offers AS (
SELECT * FROM `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_link_visit_action_preprocessed` AS llva
WHERE  idaction_event_action = 6957092
),
favorite_offers_FROM_reco_module AS (
SELECT * FROM favorite_offers AS fo
LEFT JOIN `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_action_preprocessed` AS la
ON fo.idaction_name = la.raw_data.idaction
WHERE la.tracker_data.module_name = 'Module de reco' AND (idaction_url=4394835 OR idaction_url=150307)
)
SELECT count(*)FROM favorite_offers_FROM_reco_module;

-- Number of booked offers FROM recommendation module
WITH booked_offers AS (
SELECT * FROM `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_link_visit_action_preprocessed` AS llva
WHERE  idaction_event_action = 6957147
),
booked_offers_FROM_reco_module AS (
SELECT * FROM booked_offers AS bo
LEFT JOIN `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_action_preprocessed` AS la
ON bo.idaction_name = la.raw_data.idaction
WHERE la.tracker_data.module_name = 'Module de reco' AND (idaction_url=4394835 OR idaction_url=150307)
)
SELECT count(*)FROM booked_offers_FROM_reco_module;

-- Number of viewed offers FROM recommendation module
WITH scrolls AS (
SELECT server_time, user_id,
	IF(
	    REGEXP_CONTAINS(user_id, r"^[A-Z0-9]{2,}") = True,
	    algo_reco_kpi_data.dehumanize_id(REGEXP_EXTRACT(user_id, r"^[A-Z0-9]{2,}")),
	    '') AS user_id_dehumanized
	FROM `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_link_visit_action_preprocessed` llva
	LEFT JOIN `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_visit` lv
	ON llva.idvisit = lv.idvisit
	WHERE idaction_event_action = 4394836
	AND (idaction_url=4394835 OR idaction_url=150307)
),
recommended_offers AS (
	SELECT userId, offerId, date
	FROM `pass-culture-app-projet-test.algo_reco_kpi_data.past_recommended_offers`
),
viewed_recos AS (
	SELECT * FROM (
	        SELECT CAST(userId AS STRING) AS userId, CAST(offerId AS STRING) AS offerId, date AS reco_date, server_time AS scroll_time,
	        RANK() OVER (PARTITION BY userId, date ORDER BY TIMESTAMP_DIFF(date, server_time, SECOND)) AS time_rank
	        FROM recommended_offers
		    LEFT JOIN scrolls
	        on CAST(recommended_offers.userId AS STRING) = scrolls.user_id_dehumanized
	        WHERE server_time >= date
    ) WHERE time_rank = 1
)
SELECT count(*)FROM viewed_recos;
