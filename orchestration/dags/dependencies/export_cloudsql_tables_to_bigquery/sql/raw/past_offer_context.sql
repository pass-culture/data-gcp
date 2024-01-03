SELECT
    *,
    DATE("{{ today() }}") as import_date
FROM
    `{{ bigquery_tmp_dataset }}.past_offer_context_{{ yyyymmdd(today()) }}`