select id as user_id, email, date_joined, last_login, is_superuser
from {{ source("raw", "metabase_core_user") }}
