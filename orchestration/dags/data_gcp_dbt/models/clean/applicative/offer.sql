with
    offer_rank as (
        select
            *,
            row_number() over (
                partition by offer_id order by offer_updated_date desc
            ) as row_number
        from {{ ref("int_raw__offer") }} as offer
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
