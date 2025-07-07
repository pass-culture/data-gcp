{{ config(**custom_table_config()) }}

select
    zsr.id as survey_response_id,
    zsr.ticket_id,
    zsr.requester_id as ticket_requester_id,
    zsr.assignee_id as ticket_assignee_id,
    zsr.score as survey_response_score,
    timestamp(zsr.created_at) as survey_response_created_at,
    timestamp(zsr.updated_at) as survey_response_updated_at,
    date(zsr.created_at) as survey_response_created_date
from {{ source("raw", "zendesk_survey_response") }} as zsr
qualify row_number() over (partition by zsr.id order by zsr.updated_at desc) = 1
