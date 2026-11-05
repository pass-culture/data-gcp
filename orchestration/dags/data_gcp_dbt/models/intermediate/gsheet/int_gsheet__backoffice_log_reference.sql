select
    trim(log_extract) as log_extract,
    nullif(trim(log_type), '') as log_type,
    nullif(trim(log_name), '') as log_name
from {{ source("raw_eu9", "gsheet_backoffice_log_reference") }}
where nullif(trim(log_extract), '') is not null
qualify
    row_number() over (
        partition by trim(log_extract)
        order by nullif(trim(log_name), '') is null, log_name
    )
    = 1
