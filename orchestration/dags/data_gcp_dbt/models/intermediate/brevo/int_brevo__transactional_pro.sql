select
    tag,
    template,
    user_id,
    event_date,
    sum(delivered_count) as delivered_count,
    sum(opened_count) as opened_count,
    sum(unsubscribed_count) as unsubscribed_count
from {{ source("raw", "sendinblue_transactional") }}
where tag like 'pro%'
group by tag, template, user_id, event_date
