select
    fraudulent_device_id,
    parse_date(
        '%d/%m/%Y', fraudulent_device_tagged_date
    ) as fraudulent_device_tagged_date
from {{ source("raw", "gsheet_fraudulent_device") }}
