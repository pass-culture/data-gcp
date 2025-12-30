select
    ed.educational_institution_id as institution_id,
    ed.educational_deposit_creation_date as deposit_creation_date,
    ed.educational_deposit_amount,
    ey.educational_year_beginning_date,
    ey.educational_year_expiration_date,
    ey.scholar_year,
    ed.ministry,
    case
        when
            extract(month from ed.educational_deposit_beginning_date) = 8
            and extract(month from ed.educational_deposit_expiration_date) = 12
        then 'sept-dec'
        when
            extract(month from ed.educational_deposit_beginning_date) = 12
            and extract(month from ed.educational_deposit_expiration_date) = 8
        then 'janv-aout'
        else 'all year'
    end as educational_deposit_period,
    case
        when
            (
                cast(ey.educational_year_beginning_date as date) <= current_date
                and cast(ey.educational_year_expiration_date as date) >= current_date
            )
            and (
                extract(month from ed.educational_deposit_beginning_date)
                < extract(current_date)
                and extract(month from ed.educational_deposit_beginning_date)
                >= extract(current_date)
            )
        then true
        else false
    end as is_current_deposit,
    case
        when
            (
                cast(ey.educational_year_beginning_date as date) <= current_date
                and cast(ey.educational_year_expiration_date as date) >= current_date
            )
        then true
        else false
    end as is_current_scholar_year
from {{ source("raw", "applicative_database_educational_deposit") }} as ed
left join
    {{ source("raw", "applicative_database_educational_year") }} as ey
    on ed.educational_year_id = ey.adage_id
