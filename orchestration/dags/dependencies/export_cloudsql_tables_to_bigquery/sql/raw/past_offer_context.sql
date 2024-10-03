select *, date("{{ today() }}") as import_date
from `{{ bigquery_tmp_dataset }}.past_offer_context_{{ yyyymmdd(today()) }}`
