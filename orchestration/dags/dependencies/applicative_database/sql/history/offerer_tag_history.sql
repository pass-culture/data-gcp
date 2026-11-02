select
    offerer_tag_id,
    offerer_tag_name,
    offerer_tag_label,
    offerer_tag_description,
    date_add(current_date(), interval -1 day) as partition_date
from `{{ bigquery_raw_dataset }}`.`applicative_database_offerer_tag`
