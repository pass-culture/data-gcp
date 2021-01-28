from dependencies.data_analytics.enriched_data.enriched_data_utils import (
    define_humanized_id_query,
)


def define_experimentation_sessions_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE experimentation_sessions AS (
            WITH experimentation_session AS (
                SELECT
                    isUsed AS is_used,
                    userId AS user_id,
                    ROW_NUMBER() OVER (PARTITION BY userId ORDER BY isUsed DESC) rank
                FROM {dataset}.{table_prefix}booking AS booking
                JOIN {dataset}.{table_prefix}stock AS stock ON stock.id = booking.stockId
                JOIN {dataset}.{table_prefix}offer AS offer ON offer.id = stock.offerId AND offer.type = 'ThingType.ACTIVATION'
                ORDER BY user_id, is_used DESC
            )
            SELECT
                CASE WHEN experimentation_session.is_used THEN 1 ELSE 2 END AS vague_experimentation,
                user.id AS user_id
            FROM {dataset}.{table_prefix}user AS user
            LEFT JOIN experimentation_session ON experimentation_session.user_id = user.id
            WHERE user.canBookFreeOffers AND (rank = 1 OR rank is NULL)
        );
        """


def define_activation_dates_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE activation_dates AS (
            WITH validated_activation_booking AS (
                SELECT
                    booking.dateUsed AS date_used,
                    booking.userId,
                    booking.isUsed AS is_used
                FROM {dataset}.{table_prefix}booking AS booking
                JOIN {dataset}.{table_prefix}stock AS stock ON stock.id = booking.stockId
                JOIN {dataset}.{table_prefix}offer AS offer ON stock.offerId = offer.id AND offer.type = 'ThingType.ACTIVATION'
                WHERE booking.isUsed
            )
            SELECT
                CASE WHEN validated_activation_booking.is_used THEN validated_activation_booking.date_used
                    ELSE user.dateCreated
                END AS date_activation,
                user.id as user_id
            FROM {dataset}.{table_prefix}user AS user
            LEFT JOIN validated_activation_booking ON validated_activation_booking.userId = user.id
            WHERE user.canBookFreeOffers
        );
        """


def define_date_of_first_bookings_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE date_of_first_bookings AS (
            SELECT
                booking.userId AS user_id,
                MIN(booking.dateCreated) AS date_premiere_reservation
            FROM {dataset}.{table_prefix}booking AS booking
            JOIN {dataset}.{table_prefix}stock AS stock ON stock.id = booking.stockId
            JOIN {dataset}.{table_prefix}offer AS offer ON offer.id = stock.offerId
                AND offer.type != 'ThingType.ACTIVATION'
                AND (offer.bookingEmail != 'jeux-concours@passculture.app' OR offer.bookingEmail IS NULL)
            GROUP BY user_id
        );
        """


def define_date_of_second_bookings_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE date_of_second_bookings AS (
            WITH ranked_booking_data AS (
                SELECT
                    booking.userId AS user_id,
                    booking.dateCreated AS date_creation_booking,
                    rank() OVER (PARTITION BY booking.userId ORDER BY booking.dateCreated ASC) AS rank_booking
                FROM {dataset}.{table_prefix}booking AS booking
                JOIN {dataset}.{table_prefix}stock AS stock ON stock.id = booking.stockId
                JOIN {dataset}.{table_prefix}offer AS offer ON offer.id = stock.offerId
                WHERE offer.type != 'ThingType.ACTIVATION'
                    AND (offer.bookingEmail != 'jeux-concours@passculture.app' OR offer.bookingEmail IS NULL)
            )
            SELECT
                user_id,
                date_creation_booking AS date_deuxieme_reservation
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
                    offer.type AS offer_type,
                    offer.name AS offer_name,
                    offer.id AS offer_id,
                    rank() OVER (PARTITION BY booking.userId, offer.type ORDER BY booking.dateCreated) 
                        AS rank_booking_in_cat
                FROM {dataset}.{table_prefix}booking AS booking
                JOIN {dataset}.{table_prefix}stock AS stock ON booking.stockId = stock.id
                JOIN {dataset}.{table_prefix}offer AS offer ON offer.id = stock.offerId
                WHERE offer.type NOT IN ('ThingType.ACTIVATION','EventType.ACTIVATION')
                    AND (offer.bookingEmail != 'jeux-concours@passculture.app' OR offer.bookingEmail IS NULL)
            ),
            ranked_data AS (
                SELECT
                    *,
                    rank() OVER (PARTITION BY userId ORDER BY dateCreated) AS rank_cat
                FROM dat
                WHERE rank_booking_in_cat = 1
            )
            SELECT
                userId AS user_id,
                dateCreated AS date_premiere_reservation_dans_3_categories_differentes
            FROM ranked_data
            WHERE rank_cat = 3
        );
        """


def define_number_of_bookings_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE number_of_bookings AS (
            SELECT
                booking.userId AS user_id,
                COUNT(booking.id) AS number_of_bookings
            FROM {dataset}.{table_prefix}booking AS booking
            JOIN {dataset}.{table_prefix}stock AS stock ON stock.id = booking.stockId
            JOIN {dataset}.{table_prefix}offer AS offer ON offer.id = stock.offerId
                AND offer.type != 'ThingType.ACTIVATION'
                AND (offer.bookingEmail != 'jeux-concours@passculture.app' OR offer.bookingEmail IS NULL)
            GROUP BY user_id
            ORDER BY number_of_bookings ASC
        );
        """


def define_number_of_non_cancelled_bookings_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE number_of_non_cancelled_bookings AS (
            SELECT
                booking.userId AS user_id,
                COUNT(booking.id) AS number_of_bookings
            FROM {dataset}.{table_prefix}booking AS booking
            JOIN {dataset}.{table_prefix}stock AS stock ON stock.id = booking.stockId
            JOIN {dataset}.{table_prefix}offer AS offer ON offer.id = stock.offerId
                AND offer.type != 'ThingType.ACTIVATION'
                AND (offer.bookingEmail != 'jeux-concours@passculture.app' OR offer.bookingEmail IS NULL)
                AND NOT booking.isCancelled
            GROUP BY user_id
            ORDER BY number_of_bookings ASC
        );
        """


def define_users_seniority_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE users_seniority AS (
            WITH validated_activation_booking AS (
                SELECT 
                    booking.dateUsed AS date_used,
                    booking.userId,
                    booking.isUsed AS is_used
                FROM {dataset}.{table_prefix}booking AS booking
                JOIN {dataset}.{table_prefix}stock AS stock ON stock.id = booking.stockId
                JOIN {dataset}.{table_prefix}offer AS offer ON stock.offerId = offer.id AND offer.type = 'ThingType.ACTIVATION'
                WHERE booking.isUsed
            ),
            activation_date AS (
                SELECT
                    CASE WHEN validated_activation_booking.is_used THEN validated_activation_booking.date_used
                        ELSE user.dateCreated
                    END AS date_activation,
                    user.id as user_id
                FROM {dataset}.{table_prefix}user AS user
                LEFT JOIN validated_activation_booking ON validated_activation_booking.userId = user.id
                WHERE user.canBookFreeOffers
            )
            SELECT
                DATE_DIFF(CURRENT_DATE(), CAST(activation_date.date_activation AS DATE), DAY) AS anciennete_en_jours,
                user.id as user_id
            FROM {dataset}.{table_prefix}user AS user
            LEFT JOIN activation_date ON user.id = activation_date.user_id
        );
        """


def define_actual_amount_spent_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE actual_amount_spent AS (
            SELECT
                user.id AS user_id,
                COALESCE(SUM(booking.amount * booking.quantity), 0) AS montant_reel_depense
            FROM {dataset}.{table_prefix}user AS user
            LEFT JOIN {dataset}.{table_prefix}booking AS booking ON user.id = booking.userId
                AND booking.isUsed IS TRUE 
                AND booking.isCancelled IS FALSE
            WHERE user.canBookFreeOffers
            GROUP BY user.id
        );
        """


def define_theoretical_amount_spent_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE theoretical_amount_spent AS (
            SELECT
                user.id AS user_id,
                COALESCE(SUM(booking.amount * booking.quantity), 0) AS montant_theorique_depense
            FROM {dataset}.{table_prefix}user AS user
            LEFT JOIN {dataset}.{table_prefix}booking AS booking ON user.id = booking.userId AND booking.isCancelled IS FALSE
            WHERE user.canBookFreeOffers
            GROUP BY user.id
        );
        """


def define_theoretical_amount_spent_in_digital_goods_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE theoretical_amount_spent_in_digital_goods AS (
            WITH eligible_booking AS (
                SELECT
                    booking.userId,
                    booking.amount,
                    booking.quantity
                FROM {dataset}.{table_prefix}booking AS booking
                LEFT JOIN {dataset}.{table_prefix}stock AS stock ON booking.stockId = stock.id
                LEFT JOIN {dataset}.{table_prefix}offer AS offer ON stock.offerId = offer.id
                WHERE offer.type IN ('ThingType.AUDIOVISUEL',
                                     'ThingType.JEUX_VIDEO',
                                     'ThingType.JEUX_VIDEO_ABO',
                                     'ThingType.LIVRE_AUDIO',
                                     'ThingType.LIVRE_EDITION',
                                     'ThingType.MUSIQUE',
                                     'ThingType.PRESSE_ABO')
                    AND offer.url IS NOT NULL
                    AND booking.isCancelled IS FALSE
            )
            SELECT
                user.id AS user_id,
                COALESCE(SUM(eligible_booking.amount * eligible_booking.quantity), 0.) AS depenses_numeriques
            FROM {dataset}.{table_prefix}user AS user
            LEFT JOIN eligible_booking ON user.id = eligible_booking.userId
            WHERE user.canBookFreeOffers IS TRUE
            GROUP BY user.id
        );
        """


def define_theoretical_amount_spent_in_physical_goods_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE theoretical_amount_spent_in_physical_goods AS (
            WITH eligible_booking AS (
                SELECT 
                    booking.userId,
                    booking.amount,
                    booking.quantity
                FROM {dataset}.{table_prefix}booking AS booking
                LEFT JOIN {dataset}.{table_prefix}stock AS stock ON booking.stockId = stock.id
                LEFT JOIN {dataset}.{table_prefix}offer AS offer ON stock.offerId = offer.id
                WHERE offer.type IN ('ThingType.INSTRUMENT',
                                     'ThingType.JEUX',
                                     'ThingType.LIVRE_EDITION',
                                     'ThingType.MUSIQUE',
                                     'ThingType.OEUVRE_ART',
                                     'ThingType.AUDIOVISUEL')
                    AND offer.url IS NULL
                    AND booking.isCancelled IS FALSE
            )
            SELECT 
                user.id AS user_id,
                COALESCE(SUM(eligible_booking.amount * eligible_booking.quantity), 0.) AS depenses_physiques
            FROM {dataset}.{table_prefix}user AS user
            LEFT JOIN eligible_booking ON user.id = eligible_booking.userId
            WHERE user.canBookFreeOffers IS TRUE
            GROUP BY user.id
        );
        """


def define_theoretical_amount_spent_in_outings_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE theoretical_amount_spent_in_outings AS (
            WITH eligible_booking AS (
                SELECT 
                    booking.userId,
                    booking.amount,
                    booking.quantity
                FROM {dataset}.{table_prefix}booking AS booking
                LEFT JOIN {dataset}.{table_prefix}stock AS stock ON booking.stockId = stock.id
                LEFT JOIN {dataset}.{table_prefix}offer AS offer ON stock.offerId = offer.id
                WHERE offer.type IN ('EventType.SPECTACLE_VIVANT',
                                     'EventType.CINEMA',
                                     'EventType.JEUX',
                                     'ThingType.SPECTACLE_VIVANT_ABO',
                                     'EventType.MUSIQUE',
                                     'ThingType.MUSEES_PATRIMOINE_ABO',
                                     'ThingType.CINEMA_CARD',
                                     'ThingType.PRATIQUE_ARTISTIQUE_ABO',
                                     'ThingType.CINEMA_ABO',
                                     'EventType.MUSEES_PATRIMOINE',
                                     'EventType.PRATIQUE_ARTISTIQUE',
                                     'EventType.CONFERENCE_DEBAT_DEDICACE')
                    AND booking.isCancelled IS FALSE
            )
            SELECT 
                user.id AS user_id,
                COALESCE(SUM(eligible_booking.amount * eligible_booking.quantity), 0.) AS depenses_sorties
            FROM {dataset}.{table_prefix}user AS user
            LEFT JOIN eligible_booking ON user.id = eligible_booking.userId
            WHERE user.canBookFreeOffers IS TRUE
            GROUP BY user.id
        );
        """


def define_last_booking_date_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE last_booking_date AS (
            SELECT
                booking.userId AS user_id,
                MAX(booking.dateCreated) AS date_derniere_reservation
            FROM {dataset}.{table_prefix}booking AS booking
            JOIN {dataset}.{table_prefix}stock AS stock ON stock.id = booking.stockId
            JOIN {dataset}.{table_prefix}offer AS offer ON offer.id = stock.offerId
                AND offer.type != 'ThingType.ACTIVATION'
                AND (offer.bookingEmail != 'jeux-concours@passculture.app' OR offer.bookingEmail IS NULL)
            GROUP BY user_id
        );
        """


def define_enriched_user_data_query(dataset, table_prefix=""):
    return f"""
        CREATE OR REPLACE TABLE {dataset}.enriched_user_data AS (
            SELECT
                user.id AS user_id,
                experimentation_sessions.vague_experimentation,
                user.departementCode AS departement,
                user.postalCode AS code_postal,
                user.activity AS statut,
                activation_dates.date_activation,
                CASE WHEN user.hasSeenTutorials THEN user.culturalSurveyFilledDate 
                    ELSE NULL 
                END AS date_premiere_connexion,
                date_of_first_bookings.date_premiere_reservation,
                date_of_second_bookings.date_deuxieme_reservation,
                date_of_bookings_on_third_product.date_premiere_reservation_dans_3_categories_differentes,
                COALESCE(number_of_bookings.number_of_bookings, 0) AS nombre_reservations_totales,
                COALESCE(number_of_non_cancelled_bookings.number_of_bookings, 0) AS nombre_reservations_non_annulees,
                users_seniority.anciennete_en_jours,
                actual_amount_spent.montant_reel_depense,
                theoretical_amount_spent.montant_theorique_depense,
                theoretical_amount_spent_in_digital_goods.depenses_numeriques,
                theoretical_amount_spent_in_physical_goods.depenses_physiques,
                theoretical_amount_spent_in_outings.depenses_sorties,
                user_humanized_id.humanized_id AS user_humanized_id,
                last_booking_date.date_derniere_reservation
            FROM {dataset}.{table_prefix}user AS user
            LEFT JOIN experimentation_sessions ON user.id = experimentation_sessions.user_id
            LEFT JOIN activation_dates ON user.id  = activation_dates.user_id
            LEFT JOIN date_of_first_bookings ON user.id  = date_of_first_bookings.user_id
            LEFT JOIN date_of_second_bookings ON user.id  = date_of_second_bookings.user_id
            LEFT JOIN date_of_bookings_on_third_product ON user.id = date_of_bookings_on_third_product.user_id
            LEFT JOIN number_of_bookings ON user.id = number_of_bookings.user_id
            LEFT JOIN number_of_non_cancelled_bookings ON user.id = number_of_non_cancelled_bookings.user_id
            LEFT JOIN users_seniority ON user.id = users_seniority.user_id
            LEFT JOIN actual_amount_spent ON user.id = actual_amount_spent.user_id
            LEFT JOIN theoretical_amount_spent ON user.id = theoretical_amount_spent.user_id
            LEFT JOIN theoretical_amount_spent_in_digital_goods 
                ON user.id  = theoretical_amount_spent_in_digital_goods.user_id
            LEFT JOIN theoretical_amount_spent_in_physical_goods 
                ON user.id = theoretical_amount_spent_in_physical_goods.user_id
            LEFT JOIN theoretical_amount_spent_in_outings ON user.id = theoretical_amount_spent_in_outings.user_id
            LEFT JOIN last_booking_date ON last_booking_date.user_id = user.id
            LEFT JOIN {table_prefix}user_humanized_id AS user_humanized_id ON user_humanized_id.id = user.id
            WHERE user.canBookFreeOffers
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
        {define_humanized_id_query(table=f"{table_prefix}user", dataset=dataset)}
        {define_enriched_user_data_query(dataset=dataset, table_prefix=table_prefix)}
    """
