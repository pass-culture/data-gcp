-- Number of consulted offers from recommendation module

WITH consulted_offers AS (
    SELECT
        idaction_name,
        idaction_url
    FROM `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_link_visit_action_preprocessed` AS llvap
    WHERE  idaction_event_action = 6956932                          --6956932 : ConsultOffer_FromHomepage
    AND llvap.server_time >= PARSE_TIMESTAMP('%Y%m%d',@DS_START_DATE)     -- Dates à définir sur la dashboard
    AND llvap.server_time < PARSE_TIMESTAMP('%Y%m%d',@DS_END_DATE)        -- pour gérer la période d'AB testing
), consulted_offers_from_reco_module AS (
    SELECT
    lap.raw_data.idaction
    FROM consulted_offers AS co
    INNER JOIN `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_action_preprocessed` AS lap
    ON co.idaction_name = lap.raw_data.idaction
    WHERE lap.tracker_data.module_name = 'Module de reco'           -- A MODIFIER
    AND (co.idaction_url=4394835 OR co.idaction_url=150307)
)
SELECT count(*) AS count FROM consulted_offers_from_reco_module;
