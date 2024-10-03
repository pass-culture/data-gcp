select consultation_id, consulted_entity, type
from {{ ref("int_metric__discovery_daily_consultation") }}
qualify
    row_number() over (
        partition by user_id, consulted_entity, type order by consultation_timestamp asc
    )
    = 1
