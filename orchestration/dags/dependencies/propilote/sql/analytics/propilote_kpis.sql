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
    month,
    kpi
FROM
    `{{ bigquery_tmp_dataset }}.propilote_tmp_kpis_region`
UNION
ALL
SELECT
    calculation_month,
    effect,
    question_number,
    group_type,
    dimension,
    CONCAT('DPC-D',dimension) as dimension_propilote,
    month,
    kpi
FROM
    `{{ bigquery_tmp_dataset }}.propilote_tmp_kpis_department`
UNION
ALL
SELECT
    calculation_month,
    effect,
    question_number,
    group_type,
    dimension,
    "France / Déployer le pass Culture" as dimension_propilote,
    month,
    kpi
FROM
    `{{ bigquery_tmp_dataset }}.propilote_tmp_kpis_all`