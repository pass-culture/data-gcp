select
    ed.educational_institution_id as institution_id,
    ed.educational_deposit_creation_date as deposit_creation_date,
    ed.educational_deposit_amount,
    ey.educational_year_beginning_date as educational_year_beginning_date,
    ey.educational_year_expiration_date as educational_year_expiration_date,
    ed.ministry,
    case
        when
            (
                cast(ey.educational_year_beginning_date as date) <= current_date
                and cast(ey.educational_year_expiration_date as date) >= current_date
            )
        then true
        else false
    end as is_current_deposit,
    rank() over (
        partition by ed.educational_institution_id
        order by ed.educational_deposit_creation_date, ed.educational_deposit_id
    ) as deposit_rank_asc,
    rank() over (
        partition by ed.educational_institution_id
        order by
            ed.educational_deposit_creation_date desc, ed.educational_deposit_id desc
    ) as deposit_rank_desc,
    ey.educational_year_id

from {{ source("raw", "applicative_database_educational_deposit") }} as ed
left join
    {{ source("raw", "applicative_database_educational_year") }} as ey
    on ed.educational_year_id = ey.adage_id
