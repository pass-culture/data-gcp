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
), fav_booked_offers AS (
    SELECT
        llvap.idaction_name, llvap.idvisit
    FROM passculture-data-prod.clean_prod.log_link_visit_action_preprocessed AS llvap
    WHERE  llvap.idaction_event_action = 4132912
    AND llvap.server_time >= PARSE_TIMESTAMP('%Y%m%d',@DS_START_DATE)     -- Dates à définir sur la dashboard
    AND llvap.server_time < PARSE_TIMESTAMP('%Y%m%d',@DS_END_DATE)        -- pour gérer la période d'AB testing
)
SELECT count(*) as from_fav
FROM favorite_offers_from_reco_module as a
INNER JOIN fav_booked_offers as b
ON a.idvisit = b.idvisit
