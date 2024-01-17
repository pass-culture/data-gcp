SELECT
    *,
    DATE("{{ today() }}") as import_date
FROM
    `{{ bigquery_tmp_dataset }}.past_similar_offers_{{ yyyymmdd(today()) }}`