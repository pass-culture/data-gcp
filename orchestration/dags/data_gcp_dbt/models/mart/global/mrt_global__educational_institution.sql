WITH deposit_grouped_by_institution AS (
SELECT institution_id,
    MAX(CASE WHEN deposit_rank_asc = 1 THEN ministry END) AS ministry,
    MAX(CASE WHEN deposit_rank_asc = 1 THEN deposit_creation_date END) AS first_deposit_creation_date,
    MAX(CASE WHEN is_current_deposit THEN educational_deposit_amount END) AS current_deposit_amount,
    MAX(CASE WHEN is_current_deposit THEN deposit_creation_date END) AS current_deposit_creation_date,
    SUM(educational_deposit_amount) AS total_deposit_amount,
    COUNT(*) AS total_deposits,
FROM {{ ref('int_applicative__educational_deposit') }} AS ed
GROUP BY institution_id
),

collective_booking_grouped_by_institution AS (


SELECT educational_institution_id,
    SUM(collective_stock_number_of_tickets) AS total_tickets,
    SUM(CASE WHEN is_current_year_booking THEN collective_stock_number_of_tickets END) AS total_current_year_tickets,
    SUM(CASE WHEN collective_booking_status IN ('USED', 'REIMBURSED') THEN booking_amount ELSE NULL END ) AS total_collective_real_revenue,
    SUM(CASE WHEN (collective_booking_status IN ('USED', 'REIMBURSED') AND is_current_year_booking) THEN booking_amount ELSE NULL END) AS total_current_year_collective_real_revenue,
    SUM(booking_amount) AS total_collective_theoretic_revenue,
    SUM(CASE WHEN is_current_year_booking THEN booking_amount END) AS total_current_year_collective_theoretic_revenue,
    COUNT(case when is_used_collective_booking then collective_booking_id end) as total_used_collective_bookings,
    COUNT(CASE WHEN is_used_collective_booking AND is_current_year_booking THEN collective_booking_id END) AS total_current_year_used_collective_bookings,
    COUNT(DISTINCT collective_booking_id) AS total_non_cancelled_collective_bookings,
    COUNT(DISTINCT CASE WHEN is_current_year_booking THEN collective_booking_id END) AS total_current_year_non_cancelled_collective_bookings,
    MAX(CASE WHEN collective_booking_rank_asc = 1 THEN collective_booking_creation_date END) AS first_booking_date,
    MAX(CASE WHEN collective_booking_rank_desc = 1 THEN collective_booking_creation_date END) AS last_booking_date,
    MAX(CASE WHEN collective_booking_rank_desc = 1 THEN collective_offer_subcategory_id END) AS last_category_booked
from {{ ref('mrt_global__collective_booking') }}
where collective_booking_status != 'CANCELLED'
group by educational_institution_id

),

educational_institution_student_headcount AS (
    SELECT
        institution_id,
        sum(headcount) as total_students,
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
    cb.first_booking_date,
    cb.last_booking_date,
    cb.last_category_booked,
    cb.total_non_cancelled_collective_bookings,
    cb.total_current_year_non_cancelled_collective_bookings,
    cb.total_collective_theoretic_revenue,
    cb.total_current_year_collective_theoretic_revenue,
    cb.total_used_collective_bookings,
    cb.total_current_year_used_collective_bookings,
    cb.total_collective_real_revenue,
    cb.total_current_year_collective_real_revenue,
    SAFE_DIVIDE(
        cb.total_current_year_collective_real_revenue,
        dgi.current_deposit_amount
    ) AS ratio_current_credit_utilization,
    cb.total_tickets,
    cb.total_current_year_tickets,
    ei.total_credited_beneficiaries,
    SAFE_DIVIDE(
        ei.total_credited_beneficiaries,
        sh.total_students
    ) AS ratio_beneficiary_students,
    sh.total_students,
    institution_metadata_aggregated_type.macro_institution_type,
    location_info.institution_city,
    location_info.institution_epci,
    location_info.institution_density_label,
    location_info.institution_macro_density_label,
    location_info.institution_density_level,
    location_info.institution_latitude,
    location_info.institution_longitude,
    location_info.institution_academy_name,
    location_info.institution_region_name,
    location_info.institution_in_qpv,
    location_info.institution_department_code,
    location_info.institution_internal_iris_id,
    location_info.institution_postal_code
FROM  {{ ref('int_applicative__educational_institution') }} AS ei
left join deposit_grouped_by_institution AS dgi ON dgi.institution_id = ei.educational_institution_id
left join collective_booking_grouped_by_institution as cb ON cb.educational_institution_id = ei.educational_institution_id
left join educational_institution_student_headcount AS sh ON sh.institution_id = ei.institution_id
left join  {{ source('seed','institution_metadata_aggregated_type') }} as institution_metadata_aggregated_type
    on ei.institution_type = institution_metadata_aggregated_type.institution_type
left join {{ ref('int_geo__institution_location') }} as location_info on ei.institution_id = location_info.institution_id
