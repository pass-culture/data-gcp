select *
from
    `{{ bigquery_tmp_dataset }}.offline_recommendation_{{ yyyymmdd(ds) }}_day_plus_two_after_booking`
