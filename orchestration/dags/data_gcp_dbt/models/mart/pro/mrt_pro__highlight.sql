select
    highlight_request_id,
    offer_id,
    highlight_id,
    highlight_image_id,
    highlight_name,
    highlight_description,
    highlight_beginning_date,
    highlight_ending_date,
    highlight_availability_beginning_date,
    highlight_availability_ending_date,
    highlight_communication_date
from {{ ref("int_applicative__highlight") }}
