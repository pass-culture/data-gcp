select
    tag as brevo_tag,
    template as brevo_template_id,
    venue_id,
    event_date,
    sum(delivered_count) as total_delivered,
    sum(opened_count) as total_opened,
    sum(unsubscribed_count) as total_unsubscribed
from {{ source("raw", "sendinblue_transactional") }}
where tag like 'pro%'
group by tag, template, venue_id, event_date
