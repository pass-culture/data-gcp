-- Nombre de catégories par utilisateur cliquées directement depuis le module de reco.


WITH consulted_offers AS (
    SELECT
        llvap.idaction_name,
        llvap.idaction_url,
        lvp.user_id_dehumanized
    FROM `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_link_visit_action_preprocessed` AS llvap
    INNER JOIN `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_visit_preprocessed` AS lvp
        ON lvp.idvisit = llvap.idvisit
    WHERE idaction_event_action = 6956932                           --6956932 : ConsultOffer_FromHomepage
), consulted_offers_from_reco_module AS (
    SELECT
        co.user_id_dehumanized AS user_id,
        lap.tracker_data.dehumanize_offer_id AS offer_id
    FROM consulted_offers AS co
    LEFT JOIN `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_action_preprocessed` AS lap
        ON co.idaction_name = lap.raw_data.idaction
    WHERE lap.tracker_data.module_name = 'King Kendrick'            -- A MODIFIER
    AND (co.idaction_url=4394835 OR co.idaction_url=150307)         --4394835 & 150307 = page d'accueil
), offers_with_types AS (
    SELECT
    cofrm.user_id,
    cofrm.offer_id,
    o.type
    FROM consulted_offers_from_reco_module AS cofrm
    INNER JOIN `pass-culture-app-projet-test.applicative_database.offer` o
        ON o.id = cofrm.offer_id
    GROUP BY cofrm.user_id, cofrm.offer_id, o.type
), number_types_clicked_by_user AS (
    SELECT 
        user_id,
        COUNT(DISTINCT(type)) AS number_type_clicked
    FROM offers_with_types
    GROUP BY user_id
)
SELECT 
    AVG(number_type_clicked) as average,
    MAX(number_type_clicked) as max,
    MIN(number_type_clicked) as min
FROM number_types_clicked_by_user
