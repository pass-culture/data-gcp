


SELECT
    *
FROM `{{ bigquery_analytics_dataset }}.tmp_propilote_kpis_region`
UNION ALL

SELECT
    *
FROM `{{ bigquery_analytics_dataset }}.tmp_propilote_kpis_department`
UNION ALL

SELECT
    *
FROM `{{ bigquery_analytics_dataset }}.tmp_propilote_kpis_all`

