select
    collective_additional_fee_id,
    collective_stock_id,
    collective_additional_fee_type,
    collective_additional_fee_label,
    collective_additional_fee_amount
from {{ ref("int_applicative__collective_additional_fee") }}
