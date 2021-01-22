-- Number of viewed offers from recommendation module

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
)
SELECT count(*)
FROM (
    SELECT CAST(userId AS STRING) AS userId, CAST(offerId AS STRING) AS offerId, date AS reco_date, server_time AS scroll_time,
    RANK() OVER (PARTITION BY userId, date ORDER BY TIMESTAMP_DIFF(date, server_time, SECOND)) AS time_rank
    FROM recommended_offers
    LEFT JOIN scrolls
    on CAST(recommended_offers.userId AS STRING) = scrolls.user_id_dehumanized
    WHERE server_time >= date
) WHERE time_rank = 1
