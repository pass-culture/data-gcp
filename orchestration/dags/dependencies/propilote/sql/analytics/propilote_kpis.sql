SELECT
    calculation_month,
    effect,
    question_number,
    group_type,
    dimension,
    CASE
        WHEN dimension = "Collectivités d'outre-mer" THEN "Collectivité d'outre-mer / Déployer le pass Culture" 
        ELSE CONCAT(dimension, " / Déployer le pass Culture") 
    END AS dimension_propilote,
    "Valeur Actuelle" as type,
    month,
    kpi
FROM
    `{{ bigquery_tmp_dataset }}.propilote_tmp_kpis_region`
UNION
ALL
SELECT
    dpt.calculation_month,
    dpt.effect,
    dpt.question_number,
    dpt.group_type,
    region.dep_name as dimension,
    CONCAT('DPC-D', dimension, " / Déployer le pass Culture") as dimension_propilote,
    "Valeur Actuelle" as type,
    dpt.month,
    dpt.kpi
FROM
    `{{ bigquery_tmp_dataset }}.propilote_tmp_kpis_department` dpt
LEFT JOIN `{{ bigquery_analytics_dataset }}.region_department` region on dpt.dimension = region.num_dep

UNION
ALL
SELECT
    calculation_month,
    effect,
    question_number,
    group_type,
    dimension,
    "France / Déployer le pass Culture" as dimension_propilote,
    "Valeur Actuelle" as type,
    month,
    kpi
FROM
    `{{ bigquery_tmp_dataset }}.propilote_tmp_kpis_all`