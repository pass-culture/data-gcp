select
    userid as user_id,
    discordid as discord_id,
    hasaccess as user_has_access,
    isbanned as user_is_banned,
    lastupdated as last_update_date
from {{ source("raw", "applicative_database_discord_user") }}
