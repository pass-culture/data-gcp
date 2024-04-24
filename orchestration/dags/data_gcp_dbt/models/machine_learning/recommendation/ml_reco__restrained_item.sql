{{
    config(
        materialized = "view"
    )
}}

SELECT
    eod.item_id,
    MAX(
        CASE 
            -- LVL1
            WHEN eim.titelive_gtl_id LIKE "07%" THEN TRUE -- "Religion & Esotérisme"
            WHEN eim.titelive_gtl_id LIKE "08%" THEN TRUE -- "Entreprise, économie & droit"
            WHEN eim.titelive_gtl_id LIKE "09%" THEN TRUE -- "Sciences humaines & sociales"
            WHEN eim.titelive_gtl_id LIKE "10%" THEN TRUE -- "Sciences & Techniques"
            WHEN eim.titelive_gtl_id LIKE "11%" THEN TRUE -- "Scolaire"
            WHEN eim.titelive_gtl_id LIKE "12%" THEN TRUE -- "Parascolaire"
            WHEN eim.titelive_gtl_id LIKE "13%" THEN TRUE -- "Dictionnaires / Encyclopédies / Documentation"
            -- LVL2
            WHEN eim.titelive_gtl_id LIKE "0202%" THEN TRUE -- "Eveil / Petite enfance (- de 3 ans)", 
            WHEN eim.titelive_gtl_id LIKE "0203%" THEN TRUE -- "Livres illustrés / Enfance (+ de 3 ans)", 
            WHEN eim.titelive_gtl_id LIKE "0403%" THEN TRUE -- "Arts de la table / Gastronomie"
            WHEN eim.titelive_gtl_id LIKE "0406%" THEN TRUE -- "Vie quotidienne & Bien-être",
            -- LVL3
            WHEN eim.titelive_gtl_id LIKE "030201%" THEN TRUE -- "Public averti (+ 18 ans)"
            WHEN eim.titelive_gtl_id LIKE "030403%" THEN TRUE -- "Kodomo"
            WHEN eim.titelive_gtl_id LIKE "040505%" THEN TRUE -- "Jeux"
            -- LVL4
            WHEN eim.titelive_gtl_id LIKE "01020503%" THEN TRUE -- "Thriller érotique"
            WHEN eim.titelive_gtl_id LIKE "01020607%" THEN TRUE -- "Romance Erotique"
            WHEN eim.titelive_gtl_id LIKE "01020608%" THEN TRUE -- "Dark Romance"
            WHEN eim.titelive_gtl_id LIKE "01030306%" THEN TRUE -- "Roman libertin"
            WHEN eim.titelive_gtl_id LIKE "03020112%" THEN TRUE -- "Public averti (érotique, hyper violence…)"
            WHEN eim.titelive_gtl_id LIKE "04010206%" THEN TRUE -- "Enseignement universitaire"
            
        ELSE FALSE
        END
    )
     as restrained,
    MAX(
        CASE
            -- LVL1
            WHEN eim.titelive_gtl_id LIKE "09%" THEN TRUE -- "Sciences humaines & sociales"
            WHEN eim.titelive_gtl_id LIKE "11%" THEN TRUE -- "Scolaire"
            WHEN eim.titelive_gtl_id LIKE "12%" THEN TRUE -- "Parascolaire"
            -- LVL3
            WHEN eim.titelive_gtl_id LIKE "010210%" THEN TRUE -- "Erotisme"
            WHEN eim.titelive_gtl_id LIKE "061102%" THEN TRUE -- "Nu / Charme / Erotisme"
            -- LVL4
            WHEN eim.titelive_gtl_id LIKE "01030306%" THEN TRUE -- "Roman libertin"
            WHEN eim.titelive_gtl_id LIKE "03020112%" THEN TRUE -- "Public averti (érotique, hyper violence…)"
            WHEN eim.titelive_gtl_id LIKE "04010206%" THEN TRUE -- "Enseignement universitaire"
        ELSE FALSE
        END 
    )as blocked,
FROM {{ ref('enriched_offer_data') }} eod
INNER JOIN {{ ref('item_metadata') }} eim on eim.item_id = eod.item_id
WHERE eod.offer_type_domain = "BOOK"
GROUP BY 1