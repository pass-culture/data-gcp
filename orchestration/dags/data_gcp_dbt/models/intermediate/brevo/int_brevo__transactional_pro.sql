-- one row per tag, template, recipient, offerer and event day. A day can be covered
-- by two consecutive imports, each holding a disjoint part of its events, so the
-- counts are summed over them. The recipient-to-account mapping is resolved at
-- import time: the most recent import's account is kept.
select
    tag as brevo_tag,
    template as brevo_template_id,
    target,
    email_id,
    offerer_id,
    event_date,
    array_agg(user_id ignore nulls order by execution_date desc limit 1)[
        safe_offset(0)
    ] as user_id,
    sum(delivered_count) as total_delivered,
    sum(opened_count) as total_opened,
    sum(unsubscribed_count) as total_unsubscribed
from {{ source("raw", "sendinblue_transactional") }}
where target = 'pro'
group by tag, template, target, email_id, offerer_id, event_date
