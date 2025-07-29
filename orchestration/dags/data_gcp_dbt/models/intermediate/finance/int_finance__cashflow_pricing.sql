select cashflowid, pricingid
from {{ source("raw", "applicative_database_cashflow_pricing") }}
