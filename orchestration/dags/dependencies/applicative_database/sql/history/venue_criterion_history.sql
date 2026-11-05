select
    date_add(current_date(), interval -1 day) as partition_date,
    venue_id,
    venue_criterion_id,
    criterion_id
from `{{ bigquery_raw_dataset }}`.applicative_database_venue_criterion
