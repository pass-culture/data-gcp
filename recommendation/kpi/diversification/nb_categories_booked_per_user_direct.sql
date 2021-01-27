-- Nombre de catégories par utilisateur réservées directement depuis le module de reco.


WITH booked_offers AS (
    SELECT
        llvap.idaction_name,
        llvap.idaction_url,
        lvp.user_id_dehumanized
    FROM `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_link_visit_action_preprocessed` AS llvap
    INNER JOIN `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_visit_preprocessed` AS lvp
        ON lvp.idvisit = llvap.idvisit
    WHERE idaction_event_action = 6957147                                 -- 6957147: BookOfferClick_FromHomepage
    AND llvap.server_time >= PARSE_TIMESTAMP('%Y%m%d',@DS_START_DATE)     -- Dates à définir sur la dashboard
    AND llvap.server_time < PARSE_TIMESTAMP('%Y%m%d',@DS_END_DATE)        -- pour gérer la période d'AB testing
), booked_offers_from_reco_module AS (
    SELECT
        bo.user_id_dehumanized AS user_id,
        lap.tracker_data.dehumanize_offer_id AS offer_id
    FROM booked_offers AS bo
    INNER JOIN `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_action_preprocessed` AS lap
        ON bo.idaction_name = lap.raw_data.idaction
    WHERE lap.tracker_data.module_name = 'undefined'         -- A MODIFIER
), offers_with_types AS (
    SELECT
    bofrm.user_id,
    bofrm.offer_id,
    o.type
    FROM booked_offers_from_reco_module AS bofrm
    INNER JOIN `pass-culture-app-projet-test.data_analytics.offer` o
        ON o.id = bofrm.offer_id
    GROUP BY bofrm.user_id, bofrm.offer_id, o.type
), number_types_booked_by_user AS (
    SELECT
        user_id,
        COUNT(DISTINCT(type)) AS number_of_booked_types
    FROM offers_with_types
    GROUP BY user_id
)
SELECT
    AVG(number_of_booked_types) as average,
    MAX(number_of_booked_types) as max,
    MIN(number_of_booked_types) as min
FROM number_types_booked_by_user
