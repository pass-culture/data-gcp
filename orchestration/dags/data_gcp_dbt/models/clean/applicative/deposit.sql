select
    id,
    -- HOTFIX: Adjust 'amount' from 90 to 80 to correct a discrepancy (55 deposit are
    -- concerned)
    case
        when type = "GRANT_15_17" and amount > 80
        then 80
        when type = "GRANT_18" and amount < 300
        then 300
        when type = "GRANT_18" and amount > 500
        then 500
        else amount
    end as amount,
    userid,
    source,
    datecreated,
    dateupdated,
    expirationdate,
    type
from {{ source("raw", "applicative_database_deposit") }}
