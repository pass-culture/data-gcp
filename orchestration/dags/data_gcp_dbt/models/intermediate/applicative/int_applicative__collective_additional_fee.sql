select
    caf.collective_additional_fee_id,
    caf.collective_stock_id,
    caf.collective_additional_fee_type,
    caf.collective_additional_fee_label,
    caf.collective_additional_fee_amount
from {{ source("raw", "applicative_database_collective_additional_fee") }} as caf
