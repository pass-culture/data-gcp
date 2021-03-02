from dependencies.data_analytics.enriched_data.enriched_data_utils import (
    define_humanized_id_query,
)


def define_stocks_booking_view_query(dataset, table_prefix=""):
    return f"""
        CREATE OR REPLACE TABLE {dataset}.stock_booking_information AS (
            WITH last_status AS (
                SELECT
                    DISTINCT (payment_status.paymentId),
                    payment_status.paymentId as payment_id,
                    payment_status.status, date
                FROM {dataset}.{table_prefix}payment_status AS payment_status
                ORDER BY payment_status.paymentId, date DESC
            ),  valid_payment AS (
            SELECT
                bookingId
            FROM {dataset}.{table_prefix}payment AS payment
            LEFT JOIN last_status ON last_status.payment_id = payment.id
            WHERE last_status.status != 'BANNED'
            ), booking_with_payment AS (
            SELECT
                booking.booking_id,
                booking.booking_quantity
            FROM {dataset}.{table_prefix}booking AS booking
            WHERE booking.booking_id IN(SELECT bookingId FROM valid_payment)
            )
            SELECT
                stock.stock_id,
                COALESCE(SUM(booking.booking_quantity), 0) AS booking_quantity,
                COALESCE(SUM(booking.booking_quantity * CAST(booking.booking_is_cancelled AS INT64)), 0) AS bookings_cancelled,
                COALESCE(SUM(booking_with_payment.booking_quantity), 0) AS bookings_paid
            FROM {dataset}.{table_prefix}stock AS stock
            LEFT JOIN {dataset}.{table_prefix}booking AS booking ON booking.stock_id = stock.stock_id
            LEFT JOIN booking_with_payment ON booking_with_payment.booking_id = booking.booking_id
            GROUP BY stock.stock_id );
        """


def define_available_stocks_view_query(dataset, table_prefix=""):
    return f"""
        CREATE OR REPLACE TABLE {dataset}.available_stock_information AS (
            WITH bookings_grouped_by_stock AS (
            SELECT
                booking.stock_id,
                SUM(booking.booking_quantity) as number_of_booking
            FROM {dataset}.{table_prefix}booking AS booking
            LEFT JOIN {dataset}.{table_prefix}stock AS stock ON booking.stock_id = stock.stock_id
            WHERE booking.booking_is_cancelled = False
            GROUP BY booking.stock_id
            )
            SELECT
                stock.stock_id,
            CASE
                WHEN stock.stock_quantity IS NULL THEN NULL
                ELSE GREATEST(stock.stock_quantity - COALESCE(bookings_grouped_by_stock.number_of_booking, 0), 0)
            END AS available_stock_information
            FROM {dataset}.{table_prefix}stock AS stock
            LEFT JOIN bookings_grouped_by_stock
            ON bookings_grouped_by_stock.stock_id = stock.stock_id );
        """


def define_enriched_stock_data_query(dataset, table_prefix=""):
    return f"""
        CREATE OR REPLACE TABLE {dataset}.enriched_stock_data AS (
            SELECT
                stock.stock_id,
                stock.offer_id,
                offer.offer_name,
                venue.venue_managing_offerer_id AS offerer_id,
                offer.offer_type,
                venue.venue_department_code,
                stock.stock_creation_date,
                stock.stock_booking_limit_date,
                stock.stock_beginning_date,
                available_stock_information.available_stock_information,
                stock.stock_quantity,
                stock_booking_information.booking_quantity,
                stock_booking_information.bookings_cancelled AS booking_cancelled,
                stock_booking_information.bookings_paid AS booking_paid
             FROM {dataset}.{table_prefix}stock AS stock
             LEFT JOIN {dataset}.{table_prefix}offer AS offer ON stock.offer_id = offer.offer_id
             LEFT JOIN {dataset}.{table_prefix}venue AS venue ON venue.venue_id = offer.venue_id
             LEFT JOIN {dataset}.stock_booking_information ON stock.stock_id = stock_booking_information.stock_id
             LEFT JOIN stock_humanized_id AS stock_humanized_id ON stock_humanized_id.stock_id = stock.stock_id
             LEFT JOIN {dataset}.available_stock_information
             ON stock_booking_information.stock_id = available_stock_information.stock_id);
    """


def define_enriched_stock_data_full_query(dataset, table_prefix=""):
    return f"""
        {define_stocks_booking_view_query(dataset, table_prefix=table_prefix)}
        {define_available_stocks_view_query(dataset, table_prefix=table_prefix)}
        {define_humanized_id_query(table=f"stock", dataset=dataset, table_prefix=table_prefix)}
        {define_enriched_stock_data_query(dataset, table_prefix=table_prefix)}
    """
