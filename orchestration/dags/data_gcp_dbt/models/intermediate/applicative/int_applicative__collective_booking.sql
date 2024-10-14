select
    collective_booking_id,
    date(collective_booking_creation_date) as collective_booking_creation_date,
    collective_booking_creation_date as collective_booking_created_at,
    collective_booking_used_date,
    collective_stock_id,
    venue_id,
    offerer_id,
    collective_booking_cancellation_date,
    collective_booking_cancellation_date is not null as collective_booking_is_cancelled,
    collective_booking_cancellation_limit_date,
    collective_booking_cancellation_reason,
    collective_booking_status,
    collective_booking_reimbursement_date,
    educational_institution_id,
    case
        when collective_booking_status in ('USED', 'REIMBURSED') then true else false
    end as is_used_collective_booking,
    cb.educational_year_id,
    collective_booking_confirmation_date,
    collective_booking_confirmation_limit_date,
    educational_redactor_id,
    ey.educational_year_beginning_date,
    ey.educational_year_expiration_date,
    current_date
    between ey.educational_year_beginning_date and ey.educational_year_expiration_date
    as is_current_educational_year,
    rank() over (
        partition by cb.educational_institution_id
        order by cb.collective_booking_creation_date desc
    ) as collective_booking_rank_desc,
    rank() over (
        partition by cb.educational_institution_id
        order by cb.collective_booking_creation_date asc
    ) as collective_booking_rank_asc,
from {{ source("raw", "applicative_database_collective_booking") }} as cb
left join
    {{ source("raw", "applicative_database_educational_year") }} as ey
    on cb.educational_year_id = ey.educational_year_id
