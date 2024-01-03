SELECT
    *,
    DATE("{{ today() }}") as import_date
FROM
    `{{ bigquery_tmp_dataset }}.past_recommended_offers_{{ yyyymmdd(today()) }}`