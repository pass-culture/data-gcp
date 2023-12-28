SELECT
    distinct item_id
FROM `{{ bigquery_analytics_dataset }}.enriched_offer_data` eod
INNER JOIN `{{ bigquery_analytics_dataset }}.titelive_gtl_mapping` gtl on gtl.gtl_id = eod.titelive_gtl_id
WHERE 
gtl.gtl_label_level_1 IN (
    "Sciences humaines & sociales", 
    "Scolaire", 
    "Religion & Esotérisme", 
    "Parascolaire",
    "Dictionnaires / Encyclopédies / Documentation",
    "Entreprise, économie & droit",
    "Sciences & Techniques"
) 
OR
gtl.gtl_label_level_2 IN (
    "Eveil / Petite enfance (- de 3 ans)", 
    "Livres illustrés / Enfance (+ de 3 ans)", 
    "Vie quotidienne & Bien-être",
    "Arts de la table / Gastronomie"
)
OR
gtl.gtl_label_level_3 IN (
    "Jeux",
    "Public averti (+ 18 ans)",
    "Kodomo"
)
OR
gtl.gtl_label_level_4 IN (
    "Thriller érotique", 
    "Roman libertin",
    "Public averti (érotique, hyper violence…)",
    "Enseignement universitaire"
)
