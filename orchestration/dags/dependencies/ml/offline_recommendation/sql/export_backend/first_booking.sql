select *
from
    `{{ bigquery_tmp_dataset }}.offline_recommendation_{{ yyyymmdd(ds) }}_first_booking`
