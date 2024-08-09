select
    collective_booking_id,
    DATE(collective_booking_creation_date) as collective_booking_creation_date,
    collective_booking_creation_date as collective_booking_created_at,
    collective_booking_used_date,
    collective_stock_id,
    venue_id,
    offerer_id,
    collective_booking_cancellation_date,
    collective_booking_cancellation_date is not NULL as collective_booking_is_cancelled,
    collective_booking_cancellation_limit_date,
    collective_booking_cancellation_reason,
    collective_booking_status,
    collective_booking_reimbursement_date,
    educational_institution_id,
    case when collective_booking_status in ('USED', 'REIMBURSED') then true else false end as is_used_collective_booking,
    cb.educational_year_id,
    collective_booking_confirmation_date,
    collective_booking_confirmation_limit_date,
    educational_redactor_id,
    ey.educational_year_beginning_date,
    ey.educational_year_expiration_date,
    CURRENT_DATE between ey.educational_year_beginning_date
    and ey.educational_year_expiration_date as is_current_educational_year,
    RANK() over (
        partition by educational_institution_id
        order by
            collective_booking_creation_date
    ) as collective_booking_rank
from {{ source('raw', 'applicative_database_collective_booking') }} as cb
    left join {{ source('raw', 'applicative_database_educational_year') }} as ey
        on cb.educational_year_id = ey.educational_year_id
