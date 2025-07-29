select id, pricingid, timestamp, statusbefore, statusafter, reason
from {{ source("raw", "applicative_database_pricing_log") }}
