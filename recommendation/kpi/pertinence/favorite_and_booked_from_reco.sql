WITH favorite_offers AS (
    SELECT
        llvap.idaction_name, llvap.idvisit
    FROM passculture-data-prod.clean_prod.log_link_visit_action_preprocessed AS llvap
    WHERE  llvap.idaction_event_action = 6957092                          -- 6957092 : AddFavorite_FromHomepage
), favorite_offers_from_reco_module AS (
    SELECT
        fo.idaction_name, fo.idvisit
    FROM favorite_offers AS fo
    INNER JOIN passculture-data-prod.clean_prod.log_action_preprocessed AS lap
    ON fo.idaction_name = lap.raw_data.idaction
    WHERE lap.tracker_data.module_name = 'Fais le plein de découvertes'
), booked_offers AS (
    SELECT
        llvap.idaction_name, llvap.idvisit
    FROM passculture-data-prod.clean_prod.log_link_visit_action_preprocessed AS llvap
    WHERE  llvap.idaction_event_action = 6957147                   -- 6957147: BookOfferClick_FromHomepage
    AND llvap.server_time >= PARSE_TIMESTAMP('%Y%m%d',@DS_START_DATE)     -- Dates à définir sur la dashboard
    AND llvap.server_time < PARSE_TIMESTAMP('%Y%m%d',@DS_END_DATE)        -- pour gérer la période d'AB testing
), booked_offers_from_reco_module AS (
    SELECT
        bo.idaction_name, bo.idvisit
    FROM booked_offers AS bo
    INNER JOIN passculture-data-prod.clean_prod.log_action_preprocessed AS lap
    ON bo.idaction_name = lap.raw_data.idaction
    WHERE lap.tracker_data.module_name = 'Fais le plein de découvertes'
)
SELECT count(DISTINCT(b.idvisit)) as from_reco
FROM favorite_offers_from_reco_module as a
INNER JOIN booked_offers_from_reco_module as b
ON a.idvisit = b.idvisit
