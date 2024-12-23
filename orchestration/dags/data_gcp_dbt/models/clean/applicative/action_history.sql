select
    *
from {{ source("raw", "applicative_database_action_history") }}
