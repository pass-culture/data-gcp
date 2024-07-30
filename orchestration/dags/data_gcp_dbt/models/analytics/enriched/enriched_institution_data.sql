with ranked_deposit as (
    select
        educational_deposit.educational_institution_id as institution_id,
        educational_deposit_creation_date as deposit_creation_date,
        educational_year_beginning_date as educational_year_beginning_date,
        educational_year_expiration_date as educational_year_expiration_date,
        educational_deposit_amount,
        ministry,
        case
            when (
                CAST(educational_year_beginning_date as DATE) <= CURRENT_DATE
                and CAST(educational_year_expiration_date as DATE) >= CURRENT_DATE
            ) then TRUE
            else FALSE
        end as is_current_deposit,
        RANK() over (
            partition by educational_institution_id
            order by
                educational_deposit_creation_date,
                educational_deposit_id
        ) as deposit_rank_asc,
        RANK() over (
            partition by educational_institution_id
            order by
                educational_deposit_creation_date desc,
                educational_deposit_id desc
        ) as deposit_rank_desc
    from {{ ref('educational_deposit') }} as educational_deposit
        join {{ ref('educational_year') }} as educational_year on educational_deposit.educational_year_id = educational_year.adage_id
),

first_deposit as (
    select
        institution_id,
        ministry,
        deposit_creation_date as first_deposit_creation_date
    from
        ranked_deposit
    where
        deposit_rank_asc = 1
),

current_deposit as (
    select
        institution_id,
        educational_deposit_amount as institution_current_deposit_amount,
        deposit_creation_date as current_deposit_creation_date,
        ministry
    from
        ranked_deposit
    where
        is_current_deposit is TRUE
),

all_deposits as (
    select
        institution_id,
        SUM(educational_deposit_amount) as institution_deposits_total_amount,
        COUNT(*) as institution_total_number_of_deposits
    from
        ranked_deposit
    group by
        1
),

bookings_infos as (
    select
        educational_institution.educational_institution_id as institution_id,
        collective_booking.collective_booking_id as booking_id,
        collective_stock_id,
        collective_booking.collective_booking_creation_date as booking_creation_date,
        collective_booking_status as booking_status,
        collective_booking_confirmation_date,
        collective_booking_confirmation_limit_date,
        collective_booking.educational_year_id,
        RANK() over (
            partition by educational_institution.educational_institution_id
            order by
                collective_booking.collective_booking_creation_date
        ) as booking_rank_asc,
        RANK() over (
            partition by educational_institution.educational_institution_id
            order by
                collective_booking.collective_booking_creation_date desc
        ) as booking_rank_desc,
        case
            when (
                CAST(educational_year_beginning_date as DATE) <= CURRENT_DATE
                and CAST(educational_year_expiration_date as DATE) >= CURRENT_DATE
            ) then TRUE
            else FALSE
        end as is_current_year_booking
    from {{ ref('educational_institution') }} as educational_institution
        join {{ ref('collective_booking') }} as collective_booking
            on
                educational_institution.educational_institution_id = collective_booking.educational_institution_id
                and collective_booking_status != 'CANCELLED'
        join {{ ref('educational_year') }} as educational_year on educational_year.adage_id = collective_booking.educational_year_id
),

first_booking as (
    select
        institution_id,
        booking_creation_date as first_booking_date
    from
        bookings_infos
    where
        booking_rank_asc = 1
),

last_booking as (
    select
        bookings_infos.institution_id,
        bookings_infos.booking_creation_date as last_booking_date,
        collective_offer_subcategory_id as last_category_booked
    from
        bookings_infos
        join {{ ref('collective_stock') }} as collective_stock on bookings_infos.collective_stock_id = collective_stock.collective_stock_id
        join {{ ref('collective_offer') }} as collective_offer on collective_stock.collective_offer_id = collective_offer.collective_offer_id
    where
        booking_rank_desc = 1
),

bookings_per_institution as (
    select
        bookings_infos.institution_id,
        COUNT(distinct booking_id) as nb_non_cancelled_bookings,
        COUNT(distinct case when is_current_year_booking then booking_id end) as nb_non_cancelled_bookings_current_year,
        SUM(collective_stock_price) as theoric_amount_spent,
        SUM(case when is_current_year_booking then collective_stock_price end) as theoric_amount_spent_current_year,
        COUNT(
            case

                when booking_status in ('USED', 'REIMBURSED') then 1
                else NULL
            end
        ) as nb_used_bookings,
        COUNT(
            case

                when (booking_status in ('USED', 'REIMBURSED') and is_current_year_booking) then 1
                else NULL
            end
        ) as nb_used_bookings_current_year,
        SUM(
            case
                when booking_status in ('USED', 'REIMBURSED') then collective_stock_price
                else NULL
            end
        ) as real_amount_spent,
        SUM(
            case
                when (booking_status in ('USED', 'REIMBURSED') and is_current_year_booking) then collective_stock_price
                else NULL
            end
        ) as real_amount_spent_current_year,
        SUM(collective_stock_number_of_tickets) as total_eleves_concernes,
        SUM(case when is_current_year_booking then collective_stock_number_of_tickets end) as total_eleves_concernes_current_year,
        COUNT(distinct collective_offer_subcategory_id) as nb_distinct_categories_booked
    from
        bookings_infos
        join {{ ref('collective_stock') }} as collective_stock on bookings_infos.collective_stock_id = collective_stock.collective_stock_id
        join {{ ref('collective_offer') }} as collective_offer on collective_stock.collective_offer_id = collective_offer.collective_offer_id
    group by
        1
),

students_per_institution as (
    select
        educational_institution.institution_id,
        SUM(number_of_students) as nb_of_students
    from
        {{ ref('educational_institution') }} as educational_institution
        left join {{ source('analytics','number_of_students_per_eple') }} as number_of_students_per_eple on educational_institution.institution_id = number_of_students_per_eple.institution_external_id
    group by
        1
),

students_educonnectes as (
    select
        REGEXP_EXTRACT(result_content, '"school_uai": \"(.*?)\",') as institution_external_id,
        COUNT(distinct user.user_id) as nb_jeunes_credited
    from {{ ref('beneficiary_fraud_check') }} as beneficiary_fraud_check
        left join {{ ref('user_beneficiary') }} as user
            on beneficiary_fraud_check.user_id = user.user_id
        left join {{ ref('user_suspension') }} as user_suspension
            on
                user_suspension.user_id = user.user_id
                and action_history_rk = 1
    where
        type = 'EDUCONNECT'
        and REGEXP_EXTRACT(result_content, '"school_uai": \"(.*?)\",') is not NULL
        and (
            user_is_active or user_suspension.action_history_reason = 'upon user request'
        )
    group by
        1
)

select
    educational_institution.educational_institution_id as institution_id,
    educational_institution.institution_id as institution_external_id,
    educational_institution.institution_name as institution_name,
    first_deposit.ministry as ministry,
    educational_institution.institution_type,
    eple_aggregated_type.macro_institution_type,
    institution_program.institution_program_name as institution_program_name,
    location_info.institution_academy_name,
    location_info.institution_region_name,
    educational_institution.institution_departement_code,
    educational_institution.institution_postal_code,
    location_info.institution_city,
    location_info.institution_epci,
    location_info.institution_density_label,
    location_info.institution_macro_density_label,
    location_info.institution_latitude,
    location_info.institution_longitude,
    case when location_info.institution_qpv_name is not NULL then TRUE else FALSE end as institution_in_qpv,
    first_deposit.first_deposit_creation_date,
    current_deposit.institution_current_deposit_amount,
    current_deposit.current_deposit_creation_date,
    all_deposits.institution_deposits_total_amount,
    all_deposits.institution_total_number_of_deposits,
    first_booking.first_booking_date,
    last_booking.last_booking_date,
    last_booking.last_category_booked,
    bookings_per_institution.nb_non_cancelled_bookings as nb_non_cancelled_bookings,
    bookings_per_institution.nb_non_cancelled_bookings_current_year as nb_non_cancelled_bookings_current_year,
    bookings_per_institution.theoric_amount_spent,
    bookings_per_institution.theoric_amount_spent_current_year as theoric_amount_spent_current_year,
    bookings_per_institution.nb_used_bookings,
    bookings_per_institution.nb_used_bookings_current_year as nb_used_bookings_current_year,
    bookings_per_institution.real_amount_spent,
    bookings_per_institution.real_amount_spent_current_year as real_amount_spent_current_year,
    SAFE_DIVIDE(
        bookings_per_institution.real_amount_spent_current_year,
        current_deposit.institution_current_deposit_amount
    ) as part_credit_actuel_depense_reel,
    bookings_per_institution.total_eleves_concernes as total_nb_of_tickets,
    bookings_per_institution.total_eleves_concernes_current_year as total_nb_of_tickets_current_year,
    students_per_institution.nb_of_students as total_nb_of_students_in_institution,
    students_educonnectes.nb_jeunes_credited as nb_eleves_beneficiaires,
    SAFE_DIVIDE(
        students_educonnectes.nb_jeunes_credited,
        students_per_institution.nb_of_students
    ) as part_eleves_beneficiaires
from {{ ref('educational_institution') }} as educational_institution
    left join first_deposit on educational_institution.educational_institution_id = first_deposit.institution_id
    left join current_deposit on educational_institution.educational_institution_id = current_deposit.institution_id
    left join all_deposits on educational_institution.educational_institution_id = all_deposits.institution_id
    left join first_booking on educational_institution.educational_institution_id = first_booking.institution_id
    left join last_booking on educational_institution.educational_institution_id = last_booking.institution_id
    left join bookings_per_institution on educational_institution.educational_institution_id = bookings_per_institution.institution_id
    left join students_per_institution on educational_institution.institution_id = students_per_institution.institution_id
    left join students_educonnectes on educational_institution.institution_id = students_educonnectes.institution_external_id
    left join {{ source('analytics','eple') }} as eple
        on educational_institution.institution_id = eple.id_etablissement
    left join {{ ref('institution_locations') }} as location_info
        on educational_institution.institution_id = location_info.institution_id
    left join {{ source('raw','eple_aggregated_type') }} as eple_aggregated_type
        on educational_institution.institution_type = eple_aggregated_type.institution_type
    left join {{ ref('int_applicative__institution') }} as institution_program
        on educational_institution.educational_institution_id = institution_program.institution_id
