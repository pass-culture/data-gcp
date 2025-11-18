{{
    config(
        materialized="table",
    )
}}

select
    mb_users.id as user_id,
    mb_users.email,
    mb_users.date_joined,
    mb_users.last_login,
    mb_users.is_superuser,
    contact.direction as user_direction,
    contact.team as user_team
from {{ source("raw", "metabase_core_user") }} as mb_users
left join {{ source("raw", "gsheet_company_contacts") }} as contact using (email)
