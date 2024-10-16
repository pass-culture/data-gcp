select parent, child, execution_date
from {{ source("raw", "contentful_relationship") }}
qualify row_number() over (partition by parent, child order by execution_date desc) = 1
