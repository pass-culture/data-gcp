SELECT 
    group_id
    , date
    , sent
    , direct_open
    , influenced_open
    , reengaged
    , errors
    , update_date
FROM `{{ bigquery_raw_dataset }}.batch_transac`
QUALIFY row_number() over(partition by group_id, date order by update_date desc) = 1