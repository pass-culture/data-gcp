select
    count(*) as row_count,
    countif(day is null or day != @expected_day) as invalid_day_rows,
    countif(
        source_table_name is null
        or source_table_name not in unnest(@source_table_names)
    ) as invalid_source_rows
from `{{ staging_table_id }}`
