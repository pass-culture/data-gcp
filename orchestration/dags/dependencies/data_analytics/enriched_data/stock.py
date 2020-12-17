from dependencies.data_analytics.enriched_data.enriched_data_utils import (
    define_humanized_id_query,
)


def define_stocks_booking_view_query(dataset):
    return f"""
        CREATE OR REPLACE TABLE {dataset}.stock_booking_information AS (
            WITH last_status AS (
                SELECT 
                    DISTINCT (payment_status.paymentId),
                    payment_status.paymentId as payment_id,
                    payment_status.status, date
                FROM {dataset}.payment_status
                ORDER BY payment_status.paymentId, date DESC
            ),  valid_payment AS (
            SELECT 
                bookingId
            FROM {dataset}.payment
            LEFT JOIN last_status ON last_status.payment_id = payment.id
            WHERE last_status.status != 'BANNED'
            ), booking_with_payment AS (
            SELECT
                booking.id AS booking_id, 
                booking.quantity AS booking_quantity
            FROM {dataset}.booking
            WHERE booking.id IN(SELECT bookingId FROM valid_payment)
            )
            SELECT 
                stock.id AS stock_id,
                COALESCE(SUM(booking.quantity), 0) AS booking_quantity,
                COALESCE(SUM(booking.quantity * CAST(booking.isCancelled AS INT64)), 0) AS bookings_cancelled,
                COALESCE(SUM(booking_with_payment.booking_quantity), 0) AS bookings_paid
            FROM {dataset}.stock
            LEFT JOIN {dataset}.booking ON booking.stockId = stock.id
            LEFT JOIN booking_with_payment ON booking_with_payment.booking_id = booking.id
            GROUP BY stock.id ); 
        """


def define_available_stocks_view_query(dataset):
    return f"""
        CREATE OR REPLACE TABLE {dataset}.available_stock_information AS (
            WITH bookings_grouped_by_stock AS (
            SELECT 
                booking.stockId,
                SUM(booking.quantity) as number_of_booking
            FROM {dataset}.booking
            LEFT JOIN {dataset}.stock ON booking.stockId = stock.id
            WHERE booking.isCancelled = False 
            GROUP BY booking.stockId
            )
            SELECT
                stock.id AS stock_id,
            CASE 
                WHEN stock.quantity IS NULL THEN NULL
                ELSE GREATEST(stock.quantity - COALESCE(bookings_grouped_by_stock.number_of_booking, 0), 0)
            END AS stock_disponible_reel
            FROM {dataset}.stock
            LEFT JOIN bookings_grouped_by_stock 
            ON bookings_grouped_by_stock.stockId = stock.id );
        """


def define_enriched_stock_data_query(dataset):
    return f"""
        CREATE OR REPLACE TABLE {dataset}.enriched_stock_data AS (
            SELECT
                stock.id AS stock_id,
                stock.offerId AS offer_id,
                offer.name AS nom_offre,
                venue.managingOffererId AS offerer_id,
                offer.type AS type_d_offre,
                venue.departementCode AS departement,
                stock.dateCreated AS date_creation_du_stock,
                stock.bookingLimitDatetime AS date_limite_de_reservation,
                stock.beginningDatetime AS date_debut_de_l_evenement,
                available_stock_information.stock_disponible_reel,
                stock.quantity AS stock_disponible_brut_de_reservations,
                stock_booking_information.booking_quantity AS nombre_total_de_reservations,
                stock_booking_information.bookings_cancelled AS nombre_de_reservations_annulees,
                stock_booking_information.bookings_paid AS nombre_de_reservations_ayant_un_paiement
             FROM {dataset}.stock
             LEFT JOIN {dataset}.offer ON stock.offerId = offer.id
             LEFT JOIN {dataset}.venue ON venue.id = offer.venueId
             LEFT JOIN {dataset}.stock_booking_information ON stock.id = stock_booking_information.stock_id
             LEFT JOIN {dataset}.available_stock_information ON stock_booking_information.stock_id = available_stock_information.stock_id);
    """


def define_enriched_stock_data_full_query(dataset):
    return f"""
        {define_stocks_booking_view_query(dataset)}
        {define_available_stocks_view_query(dataset)}
        {define_enriched_stock_data_query(dataset)}
    """
