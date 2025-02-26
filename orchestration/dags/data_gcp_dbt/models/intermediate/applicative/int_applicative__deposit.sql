with

    recredits_grouped_by_deposit as (
        select
            deposit_id,
            max(recredit_creation_date) as last_recredit_date,
            count(distinct recredit_id) as total_recredit,
            sum(recredit_amount) as total_recredit_amount
        from {{ source("raw", "applicative_database_recredit") }}
        group by deposit_id
    )

select
    d.id as deposit_id,
    u.user_id,
    u.user_birth_date,
    d.source,
    d.datecreated as deposit_creation_date,
    d.dateupdated as deposit_update_date,
    d.expirationdate as deposit_expiration_date,
    d.type as deposit_type,
    rd.last_recredit_date,
    rd.total_recredit,
    rd.total_recredit_amount,
    -- HOTFIX: Adjust 'amount' from 90 to 80 to correct a discrepancy (55 deposit are
    -- concerned)
    case
        when d.type = "GRANT_15_17" and d.amount > 80
        then 80
        when d.type = "GRANT_18" and d.amount < 300
        then 300
        when d.type = "GRANT_18" and d.amount > 500
        then 500
        else d.amount
    end as deposit_amount,
    case
        when lower(d.source) like "%educonnect%"
        then "EDUCONNECT"
        when lower(d.source) like "%ubble%"
        then "UBBLE"
        when
            (
                lower(d.source) like "%dms%"
                or lower(d.source) like "%démarches simplifiées%"
            )
        then "DMS"
        else d.source
    end as deposit_source,
    row_number() over (
        partition by d.userid order by d.datecreated, d.id
    ) as deposit_rank_asc,
    row_number() over (
        partition by d.userid order by d.datecreated desc, d.id desc
    ) as deposit_rank_desc
from {{ source("raw", "applicative_database_deposit") }} as d
left join {{ ref("int_applicative__user") }} as u on d.userid = u.user_id
left join recredits_grouped_by_deposit as rd on d.id = rd.deposit_id
