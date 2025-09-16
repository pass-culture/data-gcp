select
    fraudulent_booking_tag_id,
    fraudulent_booking_tag_creation_date,
    fraudulent_booking_tag_author_id,
    booking_id
from {{ ref("int_applicative__fraudulent_booking_tag") }}
