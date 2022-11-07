SELECT
    *
FROM
    `{{ bigquery_tmp_dataset }}.propilote_tmp_kpis_region`
UNION
ALL
SELECT
    *
FROM
    `{{ bigquery_tmp_dataset }}.propilote_tmp_kpis_department`
UNION
ALL
SELECT
    *
FROM
    `{{ bigquery_tmp_dataset }}.propilote_tmp_kpis_all`