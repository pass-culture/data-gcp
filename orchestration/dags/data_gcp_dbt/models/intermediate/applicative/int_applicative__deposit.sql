select
    id as deposit_id,
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
    end as deposit_amount,
    userid as user_id,
    source,
    datecreated as deposit_creation_date,
    dateupdated as deposit_update_date,
    expirationdate as deposit_expiration_date,
    type as deposit_type,
    case
        when lower(source) like "%educonnect%"
        then "EDUCONNECT"
        when lower(source) like "%ubble%"
        then "UBBLE"
        when
            (lower(source) like "%dms%" or lower(source) like "%démarches simplifiées%")
        then "DMS"
        else source
    end as deposit_source,
    row_number() over (
        partition by userid order by datecreated, id
    ) as deposit_rank_asc,
    row_number() over (
        partition by userid order by datecreated desc, id desc
    ) as deposit_rank_desc
from {{ source("raw", "applicative_database_deposit") }} as d
