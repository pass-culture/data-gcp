SELECT
    item_id,
    max(
        eim.gtl_label_level_1 IN (
            "Sciences humaines & sociales", 
            "Scolaire", 
            "Religion & Esotérisme", 
            "Parascolaire",
            "Dictionnaires / Encyclopédies / Documentation",
            "Entreprise, économie & droit",
            "Sciences & Techniques"
        ) 
        OR
        eim.gtl_label_level_2 IN (
            "Eveil / Petite enfance (- de 3 ans)", 
            "Livres illustrés / Enfance (+ de 3 ans)", 
            "Vie quotidienne & Bien-être",
            "Arts de la table / Gastronomie"
        )
        OR
        eim.gtl_label_level_3 IN (
            "Jeux",
            "Public averti (+ 18 ans)",
            "Kodomo",
            "Erotisme"
        )
        OR
        eim.gtl_label_level_4 IN (
            "Thriller érotique", 
            "Roman libertin",
            "Public averti (érotique, hyper violence…)",
            "Enseignement universitaire"
        )
    ) as restrained,
    max(
        eim.gtl_label_level_1 IN (
            "Sciences humaines & sociales", 
            "Scolaire", 
            "Parascolaire"
        )
        OR
        eim.gtl_label_level_3 IN (
            "Thriller érotique",
            "Erotisme",
            "Nu / Charme / Erotisme"
        ) 
        OR
        eim.gtl_label_level_4 IN (
            "Roman libertin",
            "Public averti (érotique, hyper violence…)",
            "Enseignement universitaire"
        )
    ) as blocked
FROM `{{ bigquery_analytics_dataset }}.enriched_offer_data` eod
INNER JOIN `{{ bigquery_analytics_dataset }}.enriched_item_metadata` eim on eim.item_id = eod.item_id
GROUP BY 1