WITH ranked_deposit AS (
    SELECT
        educational_deposit.educational_institution_id AS institution_id,
        educational_deposit_creation_date AS deposit_creation_date,
        educational_year_beginning_date AS educational_year_beginning_date,
        educational_year_expiration_date AS educational_year_expiration_date,
        educational_deposit_amount,
        ministry,
        CASE
            WHEN (
                CAST(educational_year_beginning_date AS DATE) <= CURRENT_DATE
                AND CAST(educational_year_expiration_date AS DATE) >= CURRENT_DATE
            ) THEN TRUE
            ELSE FALSE
        END AS is_current_deposit,
        RANK() OVER(
            PARTITION BY educational_institution_id
            ORDER BY
                educational_deposit_creation_date,
                educational_deposit_id
        ) AS deposit_rank_asc,
        RANK() OVER(
            PARTITION BY educational_institution_id
            ORDER BY
                educational_deposit_creation_date DESC,
                educational_deposit_id DESC
        ) AS deposit_rank_desc
    FROM
        `{{ bigquery_clean_dataset }}`.applicative_database_educational_deposit AS educational_deposit
        JOIN `{{ bigquery_clean_dataset }}`.applicative_database_educational_year AS educational_year ON educational_deposit.educational_year_id = educational_year.adage_id
),

first_deposit AS (
    SELECT
        institution_id,
        ministry,
        deposit_creation_date AS first_deposit_creation_date
    FROM
        ranked_deposit
    WHERE
        deposit_rank_asc = 1
),
current_deposit AS (
    SELECT
        institution_id,
        educational_deposit_amount AS institution_current_deposit_amount,
        deposit_creation_date AS current_deposit_creation_date,
        ministry
    FROM
        ranked_deposit
    WHERE
        is_current_deposit IS TRUE
),
all_deposits AS (
    SELECT
        institution_id,
        SUM(educational_deposit_amount) AS institution_deposits_total_amount,
        COUNT(*) AS institution_total_number_of_deposits
    FROM
        ranked_deposit
    GROUP BY
        1
),
bookings_infos AS (
    SELECT
        educational_institution.educational_institution_id AS institution_id,
        collective_booking.collective_booking_id AS booking_id,
        collective_stock_id,
        collective_booking.collective_booking_creation_date AS booking_creation_date,
        collective_booking_status AS booking_status,
        collective_booking_confirmation_date,
        collective_booking_confirmation_limit_date,
        collective_booking.educational_year_id,
        RANK() OVER(
            PARTITION BY educational_institution.educational_institution_id
            ORDER BY
                collective_booking.collective_booking_creation_date
        ) AS booking_rank_asc,
        RANK() OVER(
            PARTITION BY educational_institution.educational_institution_id
            ORDER BY
                collective_booking.collective_booking_creation_date DESC
        ) AS booking_rank_desc,
        CASE
            WHEN (
                CAST(educational_year_beginning_date AS DATE) <= CURRENT_DATE
                AND CAST(educational_year_expiration_date AS DATE) >= CURRENT_DATE
            ) THEN TRUE
            ELSE FALSE
        END AS is_current_year_booking,
    FROM
        `{{ bigquery_clean_dataset }}`.applicative_database_educational_institution AS educational_institution
        JOIN `{{ bigquery_clean_dataset }}`.applicative_database_collective_booking AS collective_booking ON educational_institution.educational_institution_id = collective_booking.educational_institution_id
        AND collective_booking_status != 'CANCELLED'
        JOIN `{{ bigquery_clean_dataset }}`.applicative_database_educational_year AS educational_year ON educational_year.adage_id = collective_booking.educational_year_id
),

first_booking AS (
    SELECT
        institution_id,
        booking_creation_date AS first_booking_date
    FROM
        bookings_infos
    WHERE
        booking_rank_asc = 1
),
last_booking AS (
    SELECT
        bookings_infos.institution_id,
        bookings_infos.booking_creation_date AS last_booking_date,
        collective_offer_subcategory_id AS last_category_booked
    FROM
        bookings_infos
        JOIN `{{ bigquery_clean_dataset }}`.applicative_database_collective_stock AS collective_stock ON bookings_infos.collective_stock_id = collective_stock.collective_stock_id
        JOIN `{{ bigquery_clean_dataset }}`.applicative_database_collective_offer AS collective_offer ON collective_stock.collective_offer_id = collective_offer.collective_offer_id
    WHERE
        booking_rank_desc = 1
),
bookings_per_institution AS (
    SELECT
        bookings_infos.institution_id,
        COUNT(DISTINCT booking_id) AS nb_non_cancelled_bookings,
        COUNT(DISTINCT CASE WHEN is_current_year_booking THEN booking_id END) AS nb_non_cancelled_bookings_current_year,    
        SUM(collective_stock_price) AS theoric_amount_spent,
        SUM(CASE WHEN is_current_year_booking THEN collective_stock_price END) AS theoric_amount_spent_current_year,
        COUNT(
            CASE

                WHEN booking_status IN ('USED', 'REIMBURSED') THEN 1
                ELSE NULL
            END
        ) AS nb_used_bookings,
        COUNT(
            CASE

                WHEN (booking_status IN ('USED', 'REIMBURSED') AND is_current_year_booking) THEN 1
                ELSE NULL
            END
        ) AS nb_used_bookings_current_year,
        SUM(
            CASE
                WHEN booking_status IN ('USED', 'REIMBURSED') THEN collective_stock_price
                ELSE NULL
            END
        ) AS real_amount_spent,
        SUM(
            CASE
                WHEN (booking_status IN ('USED', 'REIMBURSED') AND is_current_year_booking) THEN collective_stock_price
                ELSE NULL
            END
        ) AS real_amount_spent_current_year,
        SUM(collective_stock_number_of_tickets) AS total_eleves_concernes,
        SUM(CASE WHEN is_current_year_booking THEN collective_stock_number_of_tickets END) AS total_eleves_concernes_current_year,
        COUNT(DISTINCT collective_offer_subcategory_id) AS nb_distinct_categories_booked
    FROM
        bookings_infos
        JOIN `{{ bigquery_clean_dataset }}`.applicative_database_collective_stock AS collective_stock ON bookings_infos.collective_stock_id = collective_stock.collective_stock_id
        JOIN `{{ bigquery_clean_dataset }}`.applicative_database_collective_offer AS collective_offer ON collective_stock.collective_offer_id = collective_offer.collective_offer_id
    GROUP BY
        1
),
students_per_institution AS (
    SELECT
        educational_institution.institution_id,
        SUM(number_of_students) AS nb_of_students
    FROM
        `{{ bigquery_clean_dataset }}`.applicative_database_educational_institution AS educational_institution
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.number_of_students_per_eple AS number_of_students_per_eple ON educational_institution.institution_id = number_of_students_per_eple.institution_external_id
    GROUP BY
        1
),

students_educonnectes AS (
    SELECT
        REGEXP_EXTRACT(result_content, '"school_uai": \"(.*?)\",') AS institution_external_id,
        COUNT(DISTINCT user.user_id) AS nb_jeunes_credited
    FROM `{{ bigquery_clean_dataset }}`.applicative_database_beneficiary_fraud_check AS beneficiary_fraud_check
    LEFT JOIN `{{ bigquery_clean_dataset }}`.user_beneficiary AS user 
        ON beneficiary_fraud_check.user_id = user.user_id 
    LEFT JOIN `{{ bigquery_clean_dataset }}`.user_suspension as user_suspension
        ON user_suspension.user_id = user.user_id
        AND rank = 1
    WHERE
        type = 'EDUCONNECT'
        AND REGEXP_EXTRACT(result_content, '"school_uai": \"(.*?)\",') IS NOT NULL
        AND (
            user_is_active OR user_suspension.action_history_reason = 'upon user request'
        )
    GROUP BY
        1
)

SELECT
    educational_institution.educational_institution_id AS institution_id,
    educational_institution.institution_id AS institution_external_id,
    institution_name AS institution_name,
    first_deposit.ministry AS ministry,
    educational_institution.institution_type,
    region_department.academy_name AS institution_academie,
    region_department.region_name AS institution_region_name,
    educational_institution.institution_departement_code,
    institution_postal_code,
    institution_city,
    rurality.geo_type as institution_rural_level,
    first_deposit.first_deposit_creation_date,
    current_deposit.institution_current_deposit_amount,
    current_deposit.current_deposit_creation_date,
    all_deposits.institution_deposits_total_amount,
    all_deposits.institution_total_number_of_deposits,
    first_booking.first_booking_date,
    last_booking.last_booking_date,
    last_booking.last_category_booked,
    bookings_per_institution.nb_non_cancelled_bookings AS nb_non_cancelled_bookings,
    bookings_per_institution.nb_non_cancelled_bookings_current_year AS nb_non_cancelled_bookings_current_year,
    bookings_per_institution.theoric_amount_spent,
    bookings_per_institution.theoric_amount_spent_current_year AS theoric_amount_spent_current_year,
    bookings_per_institution.nb_used_bookings,
    bookings_per_institution.nb_used_bookings_current_year AS nb_used_bookings_current_year,
    bookings_per_institution.real_amount_spent,
    bookings_per_institution.real_amount_spent_current_year AS real_amount_spent_current_year,
    SAFE_DIVIDE(
        bookings_per_institution.real_amount_spent_current_year,
        current_deposit.institution_current_deposit_amount
    ) AS part_credit_actuel_depense_reel,
    bookings_per_institution.total_eleves_concernes AS total_nb_of_tickets,
    bookings_per_institution.total_eleves_concernes_current_year AS total_nb_of_tickets_current_year,
    students_per_institution.nb_of_students AS total_nb_of_students_in_institution,
    students_educonnectes.nb_jeunes_credited AS nb_eleves_beneficiaires,
    SAFE_DIVIDE(
        students_educonnectes.nb_jeunes_credited,
        students_per_institution.nb_of_students
    ) AS part_eleves_beneficiaires
FROM
    `{{ bigquery_clean_dataset }}`.applicative_database_educational_institution AS educational_institution
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.region_department ON educational_institution.institution_departement_code = region_department.num_dep
    LEFT JOIN first_deposit ON educational_institution.educational_institution_id = first_deposit.institution_id
    LEFT JOIN current_deposit ON educational_institution.educational_institution_id = current_deposit.institution_id
    LEFT JOIN all_deposits ON educational_institution.educational_institution_id = all_deposits.institution_id
    LEFT JOIN first_booking ON educational_institution.educational_institution_id = first_booking.institution_id
    LEFT JOIN last_booking ON educational_institution.educational_institution_id = last_booking.institution_id
    LEFT JOIN bookings_per_institution ON educational_institution.educational_institution_id = bookings_per_institution.institution_id
    LEFT JOIN students_per_institution ON educational_institution.institution_id = students_per_institution.institution_id
    LEFT JOIN students_educonnectes ON educational_institution.institution_id = students_educonnectes.institution_external_id
    LEFT JOIN `{{ bigquery_analytics_dataset }}.eple` as eple
        ON educational_institution.institution_id = eple.id_etablissement
    LEFT JOIN `{{ bigquery_analytics_dataset }}.rural_city_type_data` as rurality 
        ON rurality.geo_code = eple.code_commune
