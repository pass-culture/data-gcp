-- Number of favorite offers from recommendation module


WITH favorite_offers AS (
    SELECT
        llvap.idaction_name
    FROM `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_link_visit_action_preprocessed` AS llvap
    WHERE  llvap.idaction_event_action = 6957092                          -- 6957092 : AddFavorite_FromHomepage
    AND llvap.server_time >= PARSE_TIMESTAMP('%Y%m%d',@DS_START_DATE)     -- Dates à définir sur la dashboard
    AND llvap.server_time < PARSE_TIMESTAMP('%Y%m%d',@DS_END_DATE)        -- pour gérer la période d'AB testing
), favorite_offers_from_reco_module AS (
    SELECT
        fo.idaction_name
    FROM favorite_offers AS fo
    INNER JOIN `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_action_preprocessed` AS lap
    ON fo.idaction_name = lap.raw_data.idaction
    WHERE lap.tracker_data.module_name = 'Module de reco'          -- A MODIFIER
)
SELECT
    count(*) AS count
FROM favorite_offers_from_reco_module;
