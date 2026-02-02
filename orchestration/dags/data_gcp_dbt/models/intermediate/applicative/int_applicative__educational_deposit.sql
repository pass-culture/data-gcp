select
    ed.educational_deposit_id,
    ed.educational_institution_id as institution_id,
    ed.educational_deposit_creation_date,
    ed.educational_deposit_amount,
    ey.educational_year_beginning_date,
    ey.educational_year_expiration_date,
    ed.educational_deposit_beginning_date,
    ed.educational_deposit_expiration_date,
    ey.scholar_year,
    ey.educational_year_id,
    ed.ministry,
    extract(year from ed.educational_deposit_beginning_date) as calendar_year,
    case
        (
            extract(month from ed.educational_deposit_beginning_date),
            extract(month from ed.educational_deposit_expiration_date)
        )
        when (9, 12)
        then 'p1'
        when (1, 8)
        then 'p2'
        else 'all_year'
    end as educational_deposit_period,
    coalesce(
        (
            cast(ey.educational_year_beginning_date as date) <= current_date
            and cast(ey.educational_year_expiration_date as date) >= current_date
        )
        and (
            extract(month from ed.educational_deposit_beginning_date)
            <= extract(month from current_date)
            and extract(month from ed.educational_deposit_expiration_date)
            > extract(month from current_date)
        ),
        false
    ) as is_current_deposit,
    rank() over (
        partition by ed.educational_institution_id
        order by ed.educational_deposit_creation_date, ed.educational_deposit_id
    ) as deposit_rank_asc,
    rank() over (
        partition by ed.educational_institution_id
        order by
            ed.educational_deposit_creation_date desc, ed.educational_deposit_id desc
    ) as deposit_rank_desc,
    coalesce(
        (
            cast(ey.educational_year_beginning_date as date) <= current_date
            and cast(ey.educational_year_expiration_date as date) >= current_date
        ),
        false
    ) as is_current_scholar_year,
    coalesce(
        extract(year from ed.educational_deposit_beginning_date)
        = extract(year from current_date),
        false
    ) as is_current_calendar_year

from {{ source("raw", "applicative_database_educational_deposit") }} as ed
left join
    {{ source("raw", "applicative_database_educational_year") }} as ey
    on ed.educational_year_id = ey.adage_id
