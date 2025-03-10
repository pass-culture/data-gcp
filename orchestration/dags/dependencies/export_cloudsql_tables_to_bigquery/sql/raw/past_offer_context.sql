select *, date("{{ ds }}") as import_date
from `{{ bigquery_tmp_dataset }}.past_offer_context_{{ yyyymmdd(ds) }}`
