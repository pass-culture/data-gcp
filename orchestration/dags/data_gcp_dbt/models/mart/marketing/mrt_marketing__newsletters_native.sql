select
    user_id,
    brevo_template_id,
    brevo_tag,
    target,
    event_date,
    total_delivered,
    total_opened,
    total_unsubscribed,
    user_current_deposit_type,
    session_number,
    total_consultations,
    total_bookings,
    total_favorites
from {{ ref("int_brevo__transactional_native") }}
