select
    group_id, date, sent, direct_open, influenced_open, reengaged, errors, update_date
from `{{ bigquery_raw_dataset }}.batch_transac`
qualify row_number() over (partition by group_id, date order by update_date desc) = 1
