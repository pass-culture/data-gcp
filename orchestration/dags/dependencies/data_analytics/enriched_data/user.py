from dependencies.data_analytics.enriched_data.enriched_data_utils import (
    create_humanize_id_function,
    create_temp_humanize_id,
)


def define_experimentation_sessions_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE experimentation_sessions AS (
            WITH experimentation_session AS (
                SELECT
                    booking_is_used,
                    user_id,
                    ROW_NUMBER() OVER (PARTITION BY booking_id ORDER BY booking_is_used DESC) rank
                FROM {dataset}.{table_prefix}booking AS booking
                JOIN {dataset}.{table_prefix}stock AS stock ON stock.stock_id = booking.stock_id
                JOIN {dataset}.{table_prefix}offer AS offer ON offer.offer_id = stock.offer_id AND offer.offer_subcategoryId = 'ACTIVATION_THING'
                ORDER BY user_id, booking_is_used DESC
            )
            SELECT
                CASE WHEN experimentation_session.booking_is_used THEN 1 ELSE 2 END AS vague_experimentation,
                user.user_id
            FROM {dataset}.{table_prefix}user AS user
            LEFT JOIN experimentation_session ON experimentation_session.user_id = user.user_id
            WHERE user.user_is_beneficiary AND (rank = 1 OR rank is NULL)
        );
        """


def define_activation_dates_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE activation_dates AS (
            WITH ranked_bookings AS (
                SELECT
                    booking.user_id
                    ,offer.offer_subcategoryId
                    ,booking_used_date
                    ,booking_is_used
                    ,RANK() OVER (PARTITION BY booking.user_id ORDER BY booking.booking_creation_date ASC, booking.booking_id ASC) AS rank_
                FROM {dataset}.{table_prefix}booking AS booking
                JOIN {dataset}.{table_prefix}stock AS stock ON booking.stock_id = stock.stock_id
                JOIN {dataset}.{table_prefix}offer AS offer ON stock.offer_id = offer.offer_id
            )
            SELECT
                user.user_id
                ,CASE WHEN "offer_subcategoryId" = 'ACTIVATION_THING' AND booking_used_date IS NOT NULL THEN booking_used_date
                ELSE user_creation_date END AS user_activation_date
            FROM {dataset}.{table_prefix}user AS user
            LEFT JOIN ranked_bookings ON user.user_id = ranked_bookings.user_id
            AND rank_ = 1
            WHERE user.user_is_beneficiary
        );
        """


def define_date_of_first_bookings_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE date_of_first_bookings AS (
            SELECT
                booking.user_id,
                MIN(booking.booking_creation_date) AS first_booking_date
            FROM {dataset}.{table_prefix}booking AS booking
            JOIN {dataset}.{table_prefix}stock AS stock ON stock.stock_id = booking.stock_id
            JOIN {dataset}.{table_prefix}offer AS offer ON offer.offer_id = stock.offer_id
                AND offer.offer_subcategoryId != 'ACTIVATION_THING'
                AND (offer.booking_email != 'jeux-concours@passculture.app' OR offer.booking_email IS NULL)
            GROUP BY user_id
        );
        """


def define_date_of_second_bookings_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE date_of_second_bookings AS (
            WITH ranked_booking_data AS (
                SELECT
                    booking.user_id,
                    booking.booking_creation_date,
                    rank() OVER (PARTITION BY booking.user_id ORDER BY booking.booking_creation_date ASC) AS rank_booking
                FROM {dataset}.{table_prefix}booking AS booking
                JOIN {dataset}.{table_prefix}stock AS stock ON stock.stock_id = booking.stock_id
                JOIN {dataset}.{table_prefix}offer AS offer ON offer.offer_id = stock.offer_id
                WHERE offer.offer_subcategoryId != 'ACTIVATION_THING'
                AND (offer.booking_email != 'jeux-concours@passculture.app' OR offer.booking_email IS NULL)
            )
            SELECT
                user_id,
                booking_creation_date AS second_booking_date
            FROM ranked_booking_data
            WHERE rank_booking = 2
        );
        """


def define_date_of_bookings_on_third_product_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE date_of_bookings_on_third_product AS (
            WITH dat AS (
                SELECT
                    booking.*,
                    offer.offer_subcategoryId,
                    offer.offer_name,
                    offer.offer_id,
                    rank() OVER (PARTITION BY booking.user_id, offer.offer_subcategoryId ORDER BY booking.booking_creation_date)
                    AS rank_booking_in_cat
                FROM {dataset}.{table_prefix}booking AS booking
                JOIN {dataset}.{table_prefix}stock AS stock ON booking.stock_id = stock.stock_id
                JOIN {dataset}.{table_prefix}offer AS offer ON offer.offer_id = stock.offer_id
                WHERE offer.offer_subcategoryId NOT IN ('ACTIVATION_THING','ACTIVATION_THING')
                    AND (offer.booking_email != 'jeux-concours@passculture.app' OR offer.booking_email IS NULL)
            ),
            ranked_data AS (
                SELECT
                    *,
                    rank() OVER (PARTITION BY user_id ORDER BY booking_creation_date) AS rank_cat
                FROM dat
                WHERE rank_booking_in_cat = 1
            )
            SELECT
                user_id,
                booking_creation_date AS booking_on_third_product_date
            FROM ranked_data
            WHERE rank_cat = 3
        );
        """


def define_number_of_bookings_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE number_of_bookings AS (
            SELECT
                booking.user_id,
                COUNT(booking.booking_id) AS number_of_bookings
            FROM {dataset}.{table_prefix}booking AS booking
            JOIN {dataset}.{table_prefix}stock AS stock ON stock.stock_id = booking.stock_id
            JOIN {dataset}.{table_prefix}offer AS offer ON offer.offer_id = stock.offer_id
                AND offer.offer_subcategoryId != 'ACTIVATION_THING'
                AND (offer.booking_email != 'jeux-concours@passculture.app' OR offer.booking_email IS NULL)
            GROUP BY user_id
            ORDER BY number_of_bookings ASC
        );
        """


def define_number_of_non_cancelled_bookings_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE number_of_non_cancelled_bookings AS (
            SELECT
                booking.user_id,
                COUNT(booking.booking_id) AS number_of_bookings
            FROM {dataset}.{table_prefix}booking AS booking
            JOIN {dataset}.{table_prefix}stock AS stock ON stock.stock_id = booking.stock_id
            JOIN {dataset}.{table_prefix}offer AS offer ON offer.offer_id = stock.offer_id
                AND offer.offer_subcategoryId != 'ACTIVATION_THING'
                AND (offer.booking_email != 'jeux-concours@passculture.app' OR offer.booking_email IS NULL)
                AND NOT booking.booking_is_cancelled
            GROUP BY user_id
            ORDER BY number_of_bookings ASC
        );
        """


def define_users_seniority_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE users_seniority AS (
            WITH validated_activation_booking AS (
                SELECT
                    booking.booking_used_date,
                    booking.user_id,
                    booking.booking_is_used
                FROM {dataset}.{table_prefix}booking AS booking
                JOIN {dataset}.{table_prefix}stock AS stock ON stock.stock_id = booking.stock_id
                JOIN {dataset}.{table_prefix}offer AS offer ON stock.offer_id = offer.offer_id AND offer.offer_subcategoryId = 'ACTIVATION_THING'
                WHERE booking.booking_is_used
            ),
            activation_date AS (
                SELECT
                    CASE WHEN validated_activation_booking.booking_is_used THEN validated_activation_booking.booking_used_date
                        ELSE user.user_creation_date
                    END AS activation_date,
                    user.user_id
                FROM {dataset}.{table_prefix}user AS user
                LEFT JOIN validated_activation_booking ON validated_activation_booking.user_id = user.user_id
                WHERE user.user_is_beneficiary
            )
            SELECT
                DATE_DIFF(CURRENT_DATE(), CAST(activation_date.activation_date AS DATE), DAY) AS user_seniority,
                user.user_id
            FROM {dataset}.{table_prefix}user AS user
            LEFT JOIN activation_date ON user.user_id = activation_date.user_id
        );
        """


def define_actual_amount_spent_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE actual_amount_spent AS (
            SELECT
                user.user_id,
                COALESCE(SUM(booking.booking_amount * booking.booking_quantity), 0) AS actual_amount_spent
            FROM {dataset}.{table_prefix}user AS user
            LEFT JOIN {dataset}.{table_prefix}booking AS booking ON user.user_id = booking.user_id
                AND booking.booking_is_used IS TRUE
                AND booking.booking_is_cancelled IS FALSE
            WHERE user.user_is_beneficiary
            GROUP BY user.user_id
        );
        """


def define_theoretical_amount_spent_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE theoretical_amount_spent AS (
            SELECT
                user.user_id,
                COALESCE(SUM(booking.booking_amount * booking.booking_quantity), 0) AS theoretical_amount_spent
            FROM {dataset}.{table_prefix}user AS user
            LEFT JOIN {dataset}.{table_prefix}booking AS booking ON user.user_id = booking.user_id AND booking.booking_is_cancelled IS FALSE
            WHERE user.user_is_beneficiary
            GROUP BY user.user_id
        );
        """


def define_theoretical_amount_spent_in_digital_goods_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE theoretical_amount_spent_in_digital_goods AS (
            WITH eligible_booking AS (
                SELECT
                    booking.user_id,
                    booking.booking_amount,
                    booking.booking_quantity
                FROM {dataset}.{table_prefix}booking AS booking
                LEFT JOIN {dataset}.{table_prefix}stock AS stock ON booking.stock_id = stock.stock_id
                LEFT JOIN {dataset}.{table_prefix}offer AS offer ON stock.offer_id = offer.offer_id
                INNER JOIN {dataset}.subcategories AS subcategories ON offer.offer_subcategoryId = subcategories.id
                WHERE subcategories.is_digital_deposit = true
                    AND offer.offer_url IS NOT NULL
                    AND booking.booking_is_cancelled IS FALSE
            )
            SELECT
                user.user_id,
                COALESCE(SUM(eligible_booking.booking_amount * eligible_booking.booking_quantity), 0.) AS amount_spent_in_digital_goods
            FROM {dataset}.{table_prefix}user AS user
            LEFT JOIN eligible_booking ON user.user_id = eligible_booking.user_id
            WHERE user.user_is_beneficiary IS TRUE
            GROUP BY user.user_id
        );
        """


def define_theoretical_amount_spent_in_physical_goods_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE theoretical_amount_spent_in_physical_goods AS (
            WITH eligible_booking AS (
                SELECT
                    booking.user_id,
                    booking.booking_amount,
                    booking.booking_quantity
                FROM {dataset}.{table_prefix}booking AS booking
                LEFT JOIN {dataset}.{table_prefix}stock AS stock ON booking.stock_id = stock.stock_id
                LEFT JOIN {dataset}.{table_prefix}offer AS offer ON stock.offer_id = offer.offer_id
                INNER JOIN {dataset}.subcategories AS subcategories ON offer.offer_subcategoryId = subcategories.id
                WHERE subcategories.is_physical_deposit = true
                    AND offer.offer_url IS NULL
                    AND booking.booking_is_cancelled IS FALSE
            )
            SELECT
                user.user_id,
                COALESCE(SUM(eligible_booking.booking_amount * eligible_booking.booking_quantity), 0.) AS amount_spent_in_physical_goods
            FROM {dataset}.{table_prefix}user AS user
            LEFT JOIN eligible_booking ON user.user_id = eligible_booking.user_id
            WHERE user.user_is_beneficiary IS TRUE
            GROUP BY user.user_id
        );
        """


def define_theoretical_amount_spent_in_outings_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE theoretical_amount_spent_in_outings AS (
            WITH eligible_booking AS (
                SELECT
                    booking.user_id,
                    booking.booking_amount,
                    booking.booking_quantity
                FROM {dataset}.{table_prefix}booking AS booking
                LEFT JOIN {dataset}.{table_prefix}stock AS stock ON booking.stock_id = stock.stock_id
                LEFT JOIN {dataset}.{table_prefix}offer AS offer ON stock.offer_id = offer.offer_id
                INNER JOIN {dataset}.subcategories AS subcategories ON offer.offer_subcategoryId = subcategories.id
                WHERE subcategories.is_event = true
                    AND booking.booking_is_cancelled IS FALSE
            )
            SELECT
                user.user_id,
                COALESCE(SUM(eligible_booking.booking_amount * eligible_booking.booking_quantity), 0.)
                AS amount_spent_in_outings
            FROM {dataset}.{table_prefix}user AS user
            LEFT JOIN eligible_booking ON user.user_id = eligible_booking.user_id
            WHERE user.user_is_beneficiary IS TRUE
            GROUP BY user.user_id
        );
        """


def define_last_booking_date_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE last_booking_date AS (
            SELECT
                booking.user_id,
                MAX(booking.booking_creation_date) AS last_booking_date
            FROM {dataset}.{table_prefix}booking AS booking
            JOIN {dataset}.{table_prefix}stock AS stock ON stock.stock_id = booking.stock_id
            JOIN {dataset}.{table_prefix}offer AS offer ON offer.offer_id = stock.offer_id
                AND offer.offer_subcategoryId != 'ACTIVATION_THING'
                AND (offer.booking_email != 'jeux-concours@passculture.app' OR offer.booking_email IS NULL)
            GROUP BY user_id
        );
        """


def define_first_paid_booking_date_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE first_paid_booking_date AS (
            SELECT
            booking.user_id
            ,min(booking.booking_creation_date) AS booking_creation_date_first
        FROM {dataset}.{table_prefix}booking AS booking
        JOIN {dataset}.{table_prefix}stock AS stock ON stock.stock_id = booking.stock_id
        JOIN {dataset}.{table_prefix}offer AS offer ON offer.offer_id = stock.offer_id
        AND offer.offer_subcategoryId != 'ACTIVATION_THING'
        AND (offer.booking_email != 'jeux-concours@passculture.app' OR offer.booking_email IS NULL)
        AND COALESCE(booking.booking_amount,0) > 0
        GROUP BY user_id
        );
        """


def define_first_booking_type_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE first_booking_type AS (
            WITH bookings_ranked AS (
                SELECT
                    booking.booking_id
                    ,booking.user_id
                    ,offer.offer_subcategoryId
                    ,rank() over (partition by booking.user_id order by booking.booking_creation_date, booking.booking_id ASC) AS rank_booking
                FROM {dataset}.{table_prefix}booking AS booking
                JOIN {dataset}.{table_prefix}stock AS stock
                ON booking.stock_id = stock.stock_id
                JOIN {dataset}.{table_prefix}offer AS offer
                ON offer.offer_id = stock.offer_id
                AND offer.offer_subcategoryId NOT IN ('ACTIVATION_THING','ACTIVATION_EVENT')
                AND (offer.booking_email != 'jeux-concours@passculture.app' OR offer.booking_email IS NULL)
                )
            SELECT
                user_id
                ,offer_subcategoryId AS first_booking_type
            FROM bookings_ranked
            WHERE rank_booking = 1

        );
        """


def define_first_paid_booking_type_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE first_paid_booking_type AS (
            WITH paid_bookings_ranked AS (
                SELECT
                    booking.booking_id
                    ,booking.user_id
                    ,offer.offer_subcategoryId
                    ,rank() over (partition by booking.user_id order by booking.booking_creation_date) AS rank_booking
                FROM {dataset}.{table_prefix}booking AS booking
                JOIN {dataset}.{table_prefix}stock AS stock
                ON booking.stock_id = stock.stock_id
                JOIN {dataset}.{table_prefix}offer AS offer
                ON offer.offer_id = stock.offer_id
                AND offer.offer_subcategoryId NOT IN ('ACTIVATION_THING','ACTIVATION_EVENT')
                AND (offer.booking_email != 'jeux-concours@passculture.app' OR offer.booking_email IS NULL)
                AND booking.booking_amount > 0
            )
            SELECT
                user_id
                ,offer_subcategoryId AS first_paid_booking_type
            FROM paid_bookings_ranked
            WHERE rank_booking = 1

        );
        """


def define_count_distinct_types_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE count_distinct_types AS (
            SELECT
                booking.user_id
                ,COUNT(DISTINCT offer.offer_subcategoryId) AS cnt_distinct_types
            FROM {dataset}.{table_prefix}booking AS booking
            JOIN {dataset}.{table_prefix}stock AS stock
            ON booking.stock_id = stock.stock_id
            JOIN {dataset}.{table_prefix}offer AS offer
            ON offer.offer_id = stock.offer_id
            AND offer.offer_subcategoryId NOT IN ('ACTIVATION_THING','ACTIVATION_EVENT')
            AND (offer.booking_email != 'jeux-concours@passculture.app' OR offer.booking_email IS NULL)
            GROUP BY user_id
        );
        """


def define_enriched_user_data_query(dataset, table_prefix=""):
    return f"""
        CREATE OR REPLACE TABLE {dataset}.enriched_user_data AS (
            SELECT
                user.user_id,
                experimentation_sessions.vague_experimentation AS experimentation_session,
                user.user_department_code,
                user.user_postal_code,
                CASE WHEN user.user_activity in ("Alternant","Apprenti","Volontaire") THEN "Apprenti, Alternant, Volontaire en service civique rémunéré"
                    WHEN user.user_activity in ("Inactif") THEN "Inactif (ni en emploi ni au chômage), En incapacité de travailler"
                        WHEN user.user_activity in ("Étudiant") THEN "Etudiant"
                            WHEN user.user_activity in ("Chômeur", "En recherche d'emploi ou chômeur") THEN "Chômeur, En recherche d'emploi"
                                ELSE user.user_activity END AS user_activity,
                user.user_civility,
                activation_dates.user_activation_date,
                deposit.dateCreated AS user_deposit_creation_date,
                CASE WHEN user.user_has_seen_tutorials THEN user.user_cultural_survey_filled_date
                    ELSE NULL
                END AS first_connection_date,
                date_of_first_bookings.first_booking_date,
                date_of_second_bookings.second_booking_date,
                date_of_bookings_on_third_product.booking_on_third_product_date,
                COALESCE(number_of_bookings.number_of_bookings, 0) AS booking_cnt,
                COALESCE(number_of_non_cancelled_bookings.number_of_bookings, 0) AS no_cancelled_booking,
                users_seniority.user_seniority,
                actual_amount_spent.actual_amount_spent,
                theoretical_amount_spent.theoretical_amount_spent,
                theoretical_amount_spent_in_digital_goods.amount_spent_in_digital_goods,
                theoretical_amount_spent_in_physical_goods.amount_spent_in_physical_goods,
                theoretical_amount_spent_in_outings.amount_spent_in_outings,
                user_humanized_id.humanized_id AS user_humanized_id,
                last_booking_date.last_booking_date,
                region_department.region_name AS user_region_name,
                first_paid_booking_date.booking_creation_date_first,
                DATE_DIFF(date_of_first_bookings.first_booking_date, deposit.dateCreated, DAY)
                AS days_between_activation_date_and_first_booking_date,
                DATE_DIFF(first_paid_booking_date.booking_creation_date_first, deposit.dateCreated, DAY)
                AS days_between_activation_date_and_first_booking_paid,
                first_booking_type.first_booking_type,
                first_paid_booking_type.first_paid_booking_type,
                count_distinct_types.cnt_distinct_types AS cnt_distinct_type_booking,
                user.user_is_active,
                user.user_suspension_reason,
                deposit.amount AS user_deposit_initial_amount,
                deposit.expirationDate AS user_deposit_expiration_date,
                CASE WHEN TIMESTAMP(deposit.expirationDate) < CURRENT_TIMESTAMP() OR actual_amount_spent.actual_amount_spent >= deposit.amount THEN TRUE ELSE FALSE END AS user_is_former_beneficiary,
                CASE WHEN (TIMESTAMP(deposit.expirationDate) >= CURRENT_TIMESTAMP() AND actual_amount_spent.actual_amount_spent < deposit.amount) AND user_is_active THEN TRUE ELSE FALSE END AS user_is_current_beneficiary,
                user.user_age,
                user.user_birth_date
            FROM {dataset}.{table_prefix}user AS user
            LEFT JOIN experimentation_sessions ON user.user_id = experimentation_sessions.user_id
            LEFT JOIN activation_dates ON user.user_id  = activation_dates.user_id
            LEFT JOIN date_of_first_bookings ON user.user_id  = date_of_first_bookings.user_id
            LEFT JOIN date_of_second_bookings ON user.user_id  = date_of_second_bookings.user_id
            LEFT JOIN date_of_bookings_on_third_product ON user.user_id = date_of_bookings_on_third_product.user_id
            LEFT JOIN number_of_bookings ON user.user_id = number_of_bookings.user_id
            LEFT JOIN number_of_non_cancelled_bookings ON user.user_id = number_of_non_cancelled_bookings.user_id
            LEFT JOIN users_seniority ON user.user_id = users_seniority.user_id
            LEFT JOIN actual_amount_spent ON user.user_id = actual_amount_spent.user_id
            LEFT JOIN theoretical_amount_spent ON user.user_id = theoretical_amount_spent.user_id
            LEFT JOIN theoretical_amount_spent_in_digital_goods
                ON user.user_id  = theoretical_amount_spent_in_digital_goods.user_id
            LEFT JOIN theoretical_amount_spent_in_physical_goods
                ON user.user_id = theoretical_amount_spent_in_physical_goods.user_id
            LEFT JOIN theoretical_amount_spent_in_outings ON user.user_id = theoretical_amount_spent_in_outings.user_id
            LEFT JOIN last_booking_date ON last_booking_date.user_id = user.user_id
            LEFT JOIN user_humanized_id AS user_humanized_id ON user_humanized_id.user_id = user.user_id
            LEFT JOIN {dataset}.region_department ON user.user_department_code = region_department.num_dep
            LEFT JOIN first_paid_booking_date ON user.user_id = first_paid_booking_date.user_id
            LEFT JOIN first_booking_type ON user.user_id = first_booking_type.user_id
            LEFT JOIN first_paid_booking_type ON user.user_id = first_paid_booking_type.user_id
            LEFT JOIN count_distinct_types ON user.user_id = count_distinct_types.user_id
            LEFT JOIN {dataset}.{table_prefix}deposit AS deposit ON user.user_id = deposit.userId
            WHERE user.user_is_beneficiary
            AND (user.user_is_active OR user.user_suspension_reason = 'upon user request')
        );
    """


def define_enriched_user_data_full_query(dataset, table_prefix=""):
    return f"""
        {define_experimentation_sessions_query(dataset=dataset, table_prefix=table_prefix)}
        {define_activation_dates_query(dataset=dataset, table_prefix=table_prefix)}
        {define_date_of_first_bookings_query(dataset=dataset, table_prefix=table_prefix)}
        {define_date_of_second_bookings_query(dataset=dataset, table_prefix=table_prefix)}
        {define_date_of_bookings_on_third_product_query(dataset=dataset, table_prefix=table_prefix)}
        {define_number_of_bookings_query(dataset=dataset, table_prefix=table_prefix)}
        {define_number_of_non_cancelled_bookings_query(dataset=dataset, table_prefix=table_prefix)}
        {define_users_seniority_query(dataset=dataset, table_prefix=table_prefix)}
        {define_actual_amount_spent_query(dataset=dataset, table_prefix=table_prefix)}
        {define_theoretical_amount_spent_query(dataset=dataset, table_prefix=table_prefix)}
        {define_theoretical_amount_spent_in_digital_goods_query(dataset=dataset, table_prefix=table_prefix)}
        {define_theoretical_amount_spent_in_physical_goods_query(dataset=dataset, table_prefix=table_prefix)}
        {define_theoretical_amount_spent_in_outings_query(dataset=dataset, table_prefix=table_prefix)}
        {define_last_booking_date_query(dataset=dataset, table_prefix=table_prefix)}
        {create_humanize_id_function()}
        {create_temp_humanize_id(table="user", dataset=dataset, table_prefix=table_prefix)}
        {define_first_paid_booking_date_query(dataset=dataset, table_prefix=table_prefix)}
        {define_first_booking_type_query(dataset=dataset, table_prefix=table_prefix)}
        {define_first_paid_booking_type_query(dataset=dataset, table_prefix=table_prefix)}
        {define_count_distinct_types_query(dataset=dataset, table_prefix=table_prefix)}
        {define_enriched_user_data_query(dataset=dataset, table_prefix=table_prefix)}
    """
