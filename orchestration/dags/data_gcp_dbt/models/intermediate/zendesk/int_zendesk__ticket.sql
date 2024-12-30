SELECT
    id AS zendesk_ticket_id,
    created_at AS zendesk_ticket_created_at,
    updated_at AS zendesk_ticket_updated_at,
    level AS zendesk_ticket_level,
    user_id,
    technical_partner,
    typology_support_pro AS zendesk_typology_support_pro,
    typology_support_pro AS zendesk_typology_support_native,
    DATE(created_at) AS zendesk_ticket_created_date
FROM  {{ source("raw", "zendesk_ticket") }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
