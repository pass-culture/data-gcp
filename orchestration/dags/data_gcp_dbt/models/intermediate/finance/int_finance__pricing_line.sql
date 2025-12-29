select id, pricingid, amount, category
from {{ source("raw", "applicative_database_pricing_line") }}
