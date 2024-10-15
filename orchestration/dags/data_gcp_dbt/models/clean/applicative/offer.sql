with
    offer_rank as (
        select
            *,
            row_number() over (
                partition by offer_id order by offer_date_updated desc
            ) as row_number
        from {{ source("raw", "applicative_database_offer") }} as offer
        where
            offer_subcategoryid not in ('ACTIVATION_THING', 'ACTIVATION_EVENT')
            and (
                booking_email != 'jeux-concours@passculture.app'
                or booking_email is null
            )
    )

select * except (row_number)
from offer_rank
where row_number = 1
