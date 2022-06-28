def define_deposit_rank_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE ranked_deposit AS (
            SELECT 
                educational_deposit.educational_institution_id AS institution_id
                ,educational_deposit_creation_date AS deposit_creation_date
                ,educational_year_beginning_date AS educational_year_beginning_date
                ,educational_year_expiration_date AS educational_year_expiration_date
                ,educational_deposit_amount
                ,ministry
                ,CASE WHEN (CAST(educational_year_beginning_date AS DATE) <= CURRENT_DATE AND CAST(educational_year_expiration_date AS DATE) >= CURRENT_DATE) THEN TRUE ELSE FALSE END AS is_current_deposit
                ,RANK() OVER(PARTITION BY educational_institution_id ORDER BY educational_deposit_creation_date,educational_deposit_id) AS deposit_rank_asc 
                ,RANK() OVER(PARTITION BY educational_institution_id ORDER BY educational_deposit_creation_date DESC,educational_deposit_id DESC) AS deposit_rank_desc            
            FROM {dataset}.{table_prefix}educational_deposit AS educational_deposit
            JOIN {dataset}.{table_prefix}educational_year AS educational_year 
                ON educational_deposit.educational_year_id = educational_year.adage_id
        );
        """


def define_first_deposit_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE first_deposit AS (
            SELECT 
                institution_id
                ,deposit_creation_date AS first_deposit_creation_date
            FROM ranked_deposit 
            WHERE deposit_rank_asc = 1 
        );
        """


def define_current_deposit_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE current_deposit AS (
            SELECT 
                institution_id
                ,educational_deposit_amount AS institution_current_deposit_amount
                ,deposit_creation_date AS current_deposit_creation_date
                ,ministry
            FROM ranked_deposit
            WHERE is_current_deposit IS TRUE 
        );
        """


def define_all_deposits_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE all_deposits AS (
            SELECT 
                institution_id
                ,SUM(educational_deposit_amount) AS institution_deposits_total_amount
                ,COUNT(*) AS institution_total_number_of_deposits
            FROM ranked_deposit 
            GROUP BY 1 
        );
        """


def define_bookings_infos_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE bookings_infos AS (
            SELECT 
            educational_institution.educational_institution_id AS institution_id 
            ,collective_booking.collective_booking_id AS booking_id
            ,collective_stock_id
            ,collective_booking.collective_booking_creation_date AS booking_creation_date
            ,collective_booking_status AS booking_status 
            ,collective_booking_confirmation_date
            ,collective_booking_confirmation_limit_date
            ,RANK() OVER(PARTITION BY educational_institution.educational_institution_id ORDER BY collective_booking.collective_booking_creation_date) AS booking_rank_asc
            ,RANK() OVER(PARTITION BY educational_institution.educational_institution_id ORDER BY collective_booking.collective_booking_creation_date DESC) AS booking_rank_desc
        FROM {dataset}.{table_prefix}educational_institution AS educational_institution
        JOIN {dataset}.{table_prefix}collective_booking AS collective_booking 
            ON educational_institution.educational_institution_id = collective_booking.educational_institution_id
            AND collective_booking_status != 'CANCELLED' 
        ); 
        """


def define_first_booking_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE first_booking AS (   
            SELECT  
                institution_id
                ,booking_creation_date AS first_booking_date
            FROM bookings_infos
            WHERE booking_rank_asc = 1
        );
        """


def define_last_booking_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE last_booking AS (   
            SELECT  
                institution_id
                ,bookings_infos.booking_creation_date AS last_booking_date
                ,collective_offer_subcategory_id AS last_category_booked
            FROM bookings_infos 
            JOIN {dataset}.{table_prefix}collective_stock AS collective_stock
                ON bookings_infos.collective_stock_id = collective_stock.collective_stock_id
            JOIN {dataset}.{table_prefix}collective_offer AS collective_offer 
                ON collective_stock.collective_offer_id = collective_offer.collective_offer_id
            WHERE booking_rank_desc = 1
        );
        """


def define_bookings_per_institution_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE bookings_per_institution AS (   
            SELECT  
            institution_id
            ,COUNT(DISTINCT booking_id) AS nb_non_cancelled_bookings
            ,SUM(collective_stock_price) AS theoric_amount_spent
            ,COUNT(CASE WHEN booking_status IN ('USED','REIMBURSED') THEN 1 ELSE NULL END) AS nb_used_bookings
            ,SUM(CASE WHEN booking_status IN ('USED','REIMBURSED') THEN collective_stock_price ELSE NULL END) AS real_amount_spent
            ,SUM(collective_stock_number_of_tickets) AS total_eleves_concernes
            ,COUNT(DISTINCT collective_offer_subcategory_id) AS nb_distinct_categories_booked
        FROM bookings_infos
        JOIN {dataset}.{table_prefix}collective_stock AS collective_stock
            ON bookings_infos.collective_stock_id = collective_stock.collective_stock_id
        JOIN {dataset}.{table_prefix}collective_offer AS collective_offer 
            ON collective_stock.collective_offer_id = collective_offer.collective_offer_id
        GROUP BY 1
        );
        """


def define_students_per_institution_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE students_per_institution AS (  
            SELECT educational_institution.institution_id
            ,SUM(number_of_students) AS nb_of_students
        FROM {dataset}.{table_prefix}educational_institution AS educational_institution
        LEFT JOIN {dataset}.number_of_students_per_eple AS number_of_students_per_eple
            ON educational_institution.institution_id = number_of_students_per_eple.institution_external_id
        GROUP BY 1
        );
        """


def define_students_educonnectes_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE students_educonnectes AS (  
            SELECT
                  REGEXP_EXTRACT(result_content, '"school_uai": \"(.*?)\",') AS institution_external_id
                , COUNT(DISTINCT enriched_user_data.user_id) AS nb_jeunes_credited
            FROM {dataset}.{table_prefix}beneficiary_fraud_check
            LEFT JOIN {dataset}.{table_prefix}enriched_user_data ON beneficiary_fraud_check.user_id = enriched_user_data.user_id
            WHERE type = 'EDUCONNECT'
            AND REGEXP_EXTRACT(result_content, '"school_uai": \"(.*?)\",') IS NOT NULL
            GROUP BY 1
        );
        """


def define_enriched_institution_data_query(dataset, table_prefix=""):
    return f"""
        CREATE OR REPLACE TABLE {dataset}.enriched_institution_data AS (
            SELECT 
                educational_institution.educational_institution_id AS institution_id 
                ,educational_institution.institution_id AS institution_external_id 
                ,institution_name AS institution_name 
                ,current_deposit.ministry AS ministry 
                ,region_departement.region_name AS institution_region_name
                ,educational_institution.institution_departement_code
                ,institution_postal_code
                ,institution_city 
                ,first_deposit.first_deposit_creation_date
                ,current_deposit.institution_current_deposit_amount
                ,current_deposit.current_deposit_creation_date
                ,all_deposits.institution_deposits_total_amount
                ,all_deposits.institution_total_number_of_deposits
                ,first_booking.first_booking_date
                ,last_booking.last_booking_date
                ,last_booking.last_category_booked
                ,bookings_per_institution.nb_non_cancelled_bookings AS nb_non_cancelled_bookings 
                ,bookings_per_institution.theoric_amount_spent
                ,bookings_per_institution.nb_used_bookings
                ,bookings_per_institution.real_amount_spent
                ,SAFE_DIVIDE(bookings_per_institution.real_amount_spent, current_deposit.institution_current_deposit_amount) AS part_credit_actuel_depense_reel
                ,bookings_per_institution.total_eleves_concernes AS total_nb_of_tickets
                ,students_per_institution.nb_of_students AS total_nb_of_students_in_institution
                ,students_educonnectes.nb_jeunes_credited AS nb_eleves_beneficiaires
                ,SAFE_DIVIDE(students_educonnectes.nb_jeunes_credited,students_per_institution.nb_of_students) AS part_eleves_beneficiaires
            FROM {dataset}.{table_prefix}educational_institution AS educational_institution
            LEFT JOIN {dataset}.region_department ON educational_institution.institution_departement_code = region_department.num_dep
            LEFT JOIN first_deposit ON educational_institution.educational_institution_id = first_deposit.institution_id 
            LEFT JOIN current_deposit ON educational_institution.educational_institution_id = current_deposit.institution_id
            LEFT JOIN all_deposits ON educational_institution.educational_institution_id = all_deposits.institution_id
            LEFT JOIN first_booking ON educational_institution.educational_institution_id = first_booking.institution_id
            LEFT JOIN last_booking ON educational_institution.educational_institution_id = last_booking.institution_id
            LEFT JOIN bookings_per_institution ON educational_institution.educational_institution_id = bookings_per_institution.institution_id
            LEFT JOIN students_per_institution ON educational_institution.institution_id = students_per_institution.institution_id
            LEFT JOIN {dataset}.academie_dept AS academie_dept ON educational_institution.institution_departement_code = academie_dept.code_dpt
            LEFT JOIN students_educonnectes ON educational_institution.institution_id = students_educonnectes.school
    );
    """


def define_enriched_institution_data_full_query(dataset, table_prefix=""):
    return f"""
        {define_deposit_rank_query(dataset=dataset, table_prefix=table_prefix)}
        {define_first_deposit_query(dataset=dataset, table_prefix=table_prefix)}
        {define_current_deposit_query(dataset=dataset, table_prefix=table_prefix)}
        {define_all_deposits_query(dataset=dataset, table_prefix=table_prefix)}
        {define_bookings_infos_query(dataset=dataset, table_prefix=table_prefix)}
        {define_first_booking_query(dataset=dataset, table_prefix=table_prefix)}
        {define_last_booking_query(dataset=dataset, table_prefix=table_prefix)}
        {define_bookings_per_institution_query(dataset=dataset, table_prefix=table_prefix)}
        {define_students_per_institution_query(dataset=dataset, table_prefix=table_prefix)}
        {define_students_educonnectes_query(dataset=dataset, table_prefix=table_prefix)}
        {define_enriched_institution_data_query(dataset=dataset, table_prefix=table_prefix)}
    """
