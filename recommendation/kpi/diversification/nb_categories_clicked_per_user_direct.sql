-- Nombre de catégories par utilisateur cliquées directement depuis le module de reco.


WITH consulted_offers AS (
    SELECT
        llvap.idaction_name,
        llvap.idaction_url,
        lvp.user_id_dehumanized
    FROM `passculture-data-prod.clean_prod.log_link_visit_action_preprocessed` AS llvap
    INNER JOIN `passculture-data-prod.clean_prod.matomo_visits` AS lvp
        ON lvp.idvisit = llvap.idvisit
    WHERE idaction_event_action = 6956932                           -- 6956932 : ConsultOffer_FromHomepage
    AND llvap.server_time >= PARSE_TIMESTAMP('%Y%m%d',@DS_START_DATE)     -- Dates à définir sur la dashboard
    AND llvap.server_time < PARSE_TIMESTAMP('%Y%m%d',@DS_END_DATE)        -- pour gérer la période d'AB testing
), consulted_offers_from_reco_module AS (
    SELECT
        co.user_id_dehumanized AS user_id,
        lap.tracker_data.dehumanize_offer_id AS offer_id
    FROM consulted_offers AS co
    JOIN `passculture-data-prod.clean_prod.log_action_preprocessed` AS lap
        ON co.idaction_name = lap.raw_data.idaction
    WHERE lap.tracker_data.module_name = 'King Kendrick'            -- A MODIFIER
    AND (co.idaction_url=4394835 OR co.idaction_url=150307)         -- 4394835 & 150307 = page d'accueil
), offers_with_types AS (
    SELECT
    cofrm.user_id,
    cofrm.offer_id,
    o.offer_type
    FROM consulted_offers_from_reco_module AS cofrm
    INNER JOIN `passculture-data-prod.analytics_prod.applicative_database_offer` o
        ON o.offer_id = cofrm.offer_id
    GROUP BY cofrm.user_id, cofrm.offer_id, o.offer_type
), number_types_clicked_by_user AS (
    SELECT
        user_id,
        COUNT(DISTINCT(offer_type)) AS number_type_clicked
    FROM offers_with_types
    GROUP BY user_id
)
SELECT
    AVG(number_type_clicked) as average,
    MAX(number_type_clicked) as max,
    MIN(number_type_clicked) as min
FROM number_types_clicked_by_user
