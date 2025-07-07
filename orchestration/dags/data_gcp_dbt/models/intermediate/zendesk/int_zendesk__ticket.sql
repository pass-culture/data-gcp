{{ config(**custom_table_config()) }}

select
    zt.id as ticket_id,
    zt.level as ticket_level,
    zt.recipient as ticket_recipient,
    zt.user_id,
    zt.requester_id as ticket_requester_id,
    zt.assignee_id as ticket_assignee_id,
    zt.technical_partner,
    zt.typology_support_pro as zendesk_typology_support_pro,
    zt.typology_support_native as zendesk_typology_support_native,
    coalesce(zt.status, 'closed') as ticket_status,
    timestamp(zt.created_at) as ticket_created_at,
    timestamp(zt.updated_at) as ticket_updated_at,
    date(zt.created_at) as ticket_created_date
from {{ source("raw", "zendesk_ticket") }} as zt
qualify row_number() over (partition by zt.id order by zt.updated_at desc) = 1
