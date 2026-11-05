select
    offerer_tag_mapping_id,
    offerer_id,
    tag_id,
    date_add(current_date(), interval -1 day) as partition_date
from `{{ bigquery_raw_dataset }}`.`applicative_database_offerer_tag_mapping`
