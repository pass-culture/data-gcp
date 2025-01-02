select
    id as zendesk_ticket_id,
    created_at as zendesk_ticket_created_at,
    updated_at as zendesk_ticket_updated_at,
    level as zendesk_ticket_level,
    user_id,
    technical_partner,
    typology_support_pro as zendesk_typology_support_pro,
    typology_support_pro as zendesk_typology_support_native,
    date(created_at) as zendesk_ticket_created_date
from {{ source("raw", "zendesk_ticket") }}
qualify row_number() over (partition by id order by updated_at desc) = 1
