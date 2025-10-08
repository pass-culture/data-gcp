select
    fraudulent_booking_tag_id,
    fraudulent_booking_tag_author_id,
    booking_id,
    date(fraudulent_booking_tag_created_at) as fraudulent_booking_tag_creation_date
from {{ ref("raw_applicative__fraudulent_booking_tag") }}
