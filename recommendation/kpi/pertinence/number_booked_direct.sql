-- Number of booked offers from recommendation module


WITH booked_offers AS (
    SELECT
        llvap.idaction_name
    FROM `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_link_visit_action_preprocessed` AS llvap
    WHERE  llvap.idaction_event_action = 6957147                   -- 6957147: BookOfferClick_FromHomepage
    AND llvap.server_time >= PARSE_TIMESTAMP('%Y%m%d',@DS_START_DATE)     -- Dates à définir sur la dashboard
    AND llvap.server_time < PARSE_TIMESTAMP('%Y%m%d',@DS_END_DATE)        -- pour gérer la période d'AB testing
), booked_offers_from_reco_module AS (
    SELECT
        bo.idaction_name
    FROM booked_offers AS bo
    INNER JOIN `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_action_preprocessed` AS lap
    ON bo.idaction_name = lap.raw_data.idaction
    WHERE lap.tracker_data.module_name = 'Module de reco'           -- A MODIFIER
)
SELECT
    count(*) as count
FROM booked_offers_from_reco_module;
