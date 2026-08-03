select
    id as dashboard_id,
    array(
        select as struct
            json_value(parameter, '$.name') as name,  -- noqa: RF04
            json_value(parameter, '$.slug') as slug,
            json_value(parameter, '$.type') as type  -- noqa: RF04
        from unnest(json_extract_array(parameters)) as parameter
    ) as dashboard_parameters
from {{ source("raw", "metabase_report_dashboard") }}
