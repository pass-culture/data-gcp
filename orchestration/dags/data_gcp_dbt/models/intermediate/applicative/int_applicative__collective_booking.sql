select
    cb.collective_booking_id,
    cb.collective_booking_creation_date as collective_booking_created_at,
    cb.collective_booking_used_date,
    cb.collective_stock_id,
    cb.venue_id,
    cb.offerer_id,
    cb.collective_booking_cancellation_date,
    cb.collective_booking_cancellation_limit_date,
    cb.collective_booking_cancellation_reason,
    cb.collective_booking_status,
    cb.collective_booking_reimbursement_date,
    cb.educational_institution_id,
    cb.educational_year_id,
    cb.collective_booking_confirmation_date,
    cb.collective_booking_confirmation_limit_date,
    cb.educational_redactor_id,
    ey.educational_year_beginning_date,
    ey.educational_year_expiration_date,
    ey2.scholar_year as collective_booking_creation_scholar_year,
    date(cb.collective_booking_creation_date) as collective_booking_creation_date,
    cb.collective_booking_cancellation_date
    is not null as collective_booking_is_cancelled,
    current_date
    between ey2.educational_year_beginning_date and ey2.educational_year_expiration_date
    as is_current_educational_year,
    rank() over (
        partition by cb.educational_institution_id
        order by cb.collective_booking_creation_date desc
    ) as collective_booking_rank_desc,
    rank() over (
        partition by cb.educational_institution_id
        order by cb.collective_booking_creation_date asc
    ) as collective_booking_rank_asc,
    coalesce(
        cb.collective_booking_status in ('USED', 'REIMBURSED'), false
    ) as is_used_collective_booking
from {{ source("raw", "applicative_database_collective_booking") }} as cb
left join
    {{ source("raw", "applicative_database_educational_year") }} as ey
    on cb.educational_year_id = ey.educational_year_id
left join
    {{ source("raw", "applicative_database_educational_year") }} as ey2
    on date(cb.collective_booking_creation_date)
    between ey2.educational_year_beginning_date and ey2.educational_year_expiration_date
