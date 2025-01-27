select
    tag as brevo_tag,
    template as brevo_template_id,
    target,
    offerer_id,
    user_id,
    event_date,
    delivered_count as total_delivered,
    opened_count as total_opened,
    unsubscribed_count as total_unsubscribed
from {{ source("raw", "sendinblue_transactional") }}
where target = 'pro'
