SELECT 
    userId AS user_id
    , discordId AS discord_id
    , hasAccess AS user_has_access
    , isBanned AS user_is_banned
    , lastUpdated AS last_update_date
FROM {{ source("raw", "applicative_database_discord_user") }} 
