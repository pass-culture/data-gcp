select
    bank_account_link_id,
    venue_id,
    bank_account_id,
    bank_account_link_beginning_date,
    bank_account_link_ending_date
from {{ source("raw", "applicative_database_venue_bank_account_link") }}