with

    recredits_grouped_by_deposit as (
        select
            deposit_id,
            max(date(recredit_creation_date)) as last_recredit_date,
            count(distinct recredit_id) as total_recredit,
            sum(recredit_amount) as total_recredit_amount,
            -- Exclude PREVIOUS_DEPOSIT transfers to avoid double-counting when
            -- summing across deposits
            sum(
                case
                    when recredit_type != 'PREVIOUS_DEPOSIT' then recredit_amount else 0
                end
            ) as total_recredit_amount_excluding_transfers
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
    {{ calculate_exact_age("d.datecreated", "u.user_birth_date") }}
    as user_age_at_deposit,
    -- HOTFIX: Adjust 'amount' from 90 to 80 to correct a discrepancy (55 deposit are
    -- concerned)
    case
        when d.type = 'GRANT_15_17' and d.amount > 80
        then 80
        when d.type = 'GRANT_18' and d.amount < 300
        then 300
        when d.type = 'GRANT_18' and d.amount > 500
        then 500
        else d.amount
    end as deposit_amount,
    case
        when lower(d.source) like '%educonnect%'
        then 'EDUCONNECT'
        when lower(d.source) like '%ubble%'
        then 'UBBLE'
        when
            (
                lower(d.source) like '%dms%'
                or lower(d.source) like '%démarches simplifiées%'
            )
        then 'DMS'
        else d.source
    end as deposit_source,
    row_number() over (
        partition by d.userid order by d.datecreated, d.id
    ) as deposit_rank_asc,
    row_number() over (
        partition by d.userid order by d.datecreated desc, d.id desc
    ) as deposit_rank_desc,
    case
        when d.type = 'GRANT_15_17'
        then '15_17_pre_reform'
        when d.type = 'GRANT_18' and d.amount <= 300
        then '18_pre_reform'
        when d.type = 'GRANT_18' and d.amount > 300
        then '18_experiment_phase'
        when d.type = 'GRANT_17_18' and d.amount < 150
        then '17_post_reform'
        when d.type = 'GRANT_17_18' and d.amount >= 150
        then '18_post_reform'
        when d.type = 'GRANT_FREE' and d.amount = 0
        then '15_16_post_reform'
    end as deposit_reform_category
from {{ source("raw", "applicative_database_deposit") }} as d
left join {{ ref("int_applicative__user") }} as u on d.userid = u.user_id
left join recredits_grouped_by_deposit as rd on d.id = rd.deposit_id
