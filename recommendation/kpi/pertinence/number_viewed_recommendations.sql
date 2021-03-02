-- Number of viewed offers from recommendation module

WITH scrolls AS (
    SELECT
        server_time,
	    user_id_dehumanized
	FROM `passculture-data-prod.clean_prod.log_link_visit_action_preprocessed` llvap
	JOIN `passculture-data-prod.clean_prod.log_visit_preprocessed` lvp
	    ON lvp.idvisit = llvap.idvisit
	WHERE llvap.idaction_event_action = 4394836                 --4394836 = AllModulesSeen
	AND (idaction_url=4394835 OR idaction_url=150307)           --4394835 & 150307 = page d'accueil
    AND llvap.server_time >= PARSE_TIMESTAMP('%Y%m%d',@DS_START_DATE)     -- Dates à définir sur la dashboard
    AND llvap.server_time < PARSE_TIMESTAMP('%Y%m%d',@DS_END_DATE)        -- pour gérer la période d'AB testing
),
recommended_offers AS (
	SELECT userId, offerId, date
	FROM `passculture-data-prod.raw_prod.past_recommended_offers`
)
SELECT count(*)
FROM (
    SELECT CAST(userId AS STRING) AS userId, CAST(offerId AS STRING) AS offerId, date AS reco_date, server_time AS scroll_time,
    RANK() OVER (PARTITION BY userId, date ORDER BY TIMESTAMP_DIFF(date, server_time, SECOND)) AS time_rank
    FROM recommended_offers
    INNER JOIN scrolls
    on CAST(recommended_offers.userId AS STRING) = scrolls.user_id_dehumanized
    WHERE server_time >= date
) WHERE time_rank = 1
