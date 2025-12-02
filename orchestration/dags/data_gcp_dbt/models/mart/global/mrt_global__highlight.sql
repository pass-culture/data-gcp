select
    offer_id,
    highlight_id,
    mediation_uuid,
    highlight_name,
    highlight_description,
    highlight_beginning_date,
    highlight_ending_date,
    highlight_beginning_date,
    highlight_availability_ending_date,
    highlight_availability_communication_date
from {{ ref("int_applicative__highlight") }}
