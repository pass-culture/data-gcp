WITH deposit_grouped_by_institution AS (
SELECT institution_id,
    MAX(CASE WHEN deposit_rank_asc = 1 THEN ministry END) AS ministry,
    MAX(CASE WHEN deposit_rank_asc = 1 THEN deposit_creation_date END) AS first_deposit_creation_date,
    MAX(CASE WHEN is_current_deposit THEN educational_deposit_amount END) AS current_deposit_amount,
    MAX(CASE WHEN is_current_deposit THEN deposit_creation_date END) AS current_deposit_creation_date,
    SUM(educational_deposit_amount) AS total_deposit_amount,
    COUNT(*) AS total_deposits,
    MAX(CASE WHEN booking_rank_asc = 1 THEN collective_booking_creation_date END) AS first_booking_date,
    MAX(CASE WHEN booking_rank_desc = 1 THEN collective_booking_creation_date END) AS last_booking_date,
    MAX(CASE WHEN booking_rank_desc = 1 THEN collective_offer_subcategory_id END) AS last_category_booked,
    COUNT(DISTINCT collective_booking_id) AS total_non_cancelled_collective_bookings,
    COUNT(DISTINCT CASE WHEN ed.is_current_year_booking THEN collective_booking_id END) AS total_current_year_non_cancelled_bookings,
    SUM(booking_amount) AS total_collective_theoretic_revenue,
    SUM(CASE WHEN ed.is_current_year_booking THEN booking_amount END) AS total_current_year_collective_theoretic_revenue,
    COUNT(case when is_used_collective_booking then collective_booking_id end) as total_used_collective_bookings,
    COUNT(CASE WHEN is_used_collective_booking AND ed.is_current_year_booking THEN collective_booking_id END) AS total_current_year_used_collective_bookings,
    SUM(CASE WHEN collective_booking_status IN ('USED', 'REIMBURSED') THEN booking_amount ELSE NULL END ) AS total_collective_real_revenue,
    SUM(CASE WHEN (collective_booking_status IN ('USED', 'REIMBURSED') AND ed.is_current_year_booking) THEN booking_amount ELSE NULL END) AS total_current_year_collective_real_revenue,
    SUM(collective_stock_number_of_tickets) AS total_tickets,
    SUM(CASE WHEN ed.is_current_year_booking THEN collective_stock_number_of_tickets END) AS total_current_year_tickets,
FROM {{ ref('int_applicative__educational_deposit') }} AS ed
LEFT JOIN {{ ref('mrt_global__collective_booking') }} AS cb ON ed.institution_id = cb.educational_institution_id
       and cb.collective_booking_status != 'CANCELLED'
GROUP BY institution_id
),

educational_institution_student_headcount AS (
    SELECT
        institution_id,
        sum(headcount) as total_students,
        avg(amount_per_student) as average_outing_budget_per_student
    FROM {{ ref("int_gsheet__educational_institution_student_headcount") }}
    GROUP BY institution_id
)

SELECT
    ei.educational_institution_id AS institution_id,
    ei.institution_id AS institution_external_id,
    ei.institution_name AS institution_name,
    dgi.ministry,
    ei.institution_type,
    ei.institution_program_name,
    dgi.first_deposit_creation_date,
    dgi.current_deposit_amount,
    dgi.current_deposit_creation_date,
    dgi.total_deposit_amount,
    dgi.total_deposits,
    dgi.first_booking_date,
    dgi.last_booking_date,
    dgi.last_category_booked,
    dgi.total_non_cancelled_collective_bookings,
    dgi.total_current_year_non_cancelled_bookings,
    dgi.total_collective_theoretic_revenue,
    dgi.total_current_year_collective_theoretic_revenue,
    dgi.total_used_collective_bookings,
    dgi.total_current_year_used_collective_bookings,
    dgi.total_collective_real_revenue,
    dgi.total_current_year_collective_real_revenue,
    SAFE_DIVIDE(
        dgi.total_current_year_collective_real_revenue,
        dgi.current_deposit_amount
    ) AS ratio_current_credit_utilization,
    dgi.total_tickets,
    dgi.total_current_year_tickets,
    ei.total_credited_beneficiaries,
    SAFE_DIVIDE(
        ei.total_credited_beneficiaries,
        sh.total_students
    ) AS ratio_beneficiary_students,
    sh.total_students,
    sh.average_outing_budget_per_student,
    institution_metadata_aggregated_type.macro_institution_type,
    location_info.institution_city,
    location_info.institution_epci,
    location_info.institution_density_label,
    location_info.institution_macro_density_label,
    location_info.institution_latitude,
    location_info.institution_longitude,
    location_info.institution_academy_name,
    location_info.institution_region_name,
    case when location_info.institution_qpv_name is not null then TRUE ELSE FALSE END AS institution_in_qpv
FROM  {{ ref('int_applicative__educational_institution') }} AS ei
left join deposit_grouped_by_institution AS dgi ON dgi.institution_id = ei.educational_institution_id
left join educational_institution_student_headcount AS sh ON sh.institution_id = ei.educational_institution_id
left join  {{ source('seed','institution_metadata_aggregated_type') }} as institution_metadata_aggregated_type
    on ei.institution_type = institution_metadata_aggregated_type.institution_type
left join {{ ref('institution_locations') }} as location_info on ei.institution_id = location_info.institution_id
