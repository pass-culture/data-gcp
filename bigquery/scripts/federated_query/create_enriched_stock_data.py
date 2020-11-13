import sys

from google.cloud import bigquery

from bigquery.utils import run_query
from set_env import set_env_vars
from bigquery.config import MIGRATION_ENRICHED_STOCK_DATA

import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

logger = logging.getLogger()


def create_stocks_booking_view(dataset):
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


def create_available_stocks_view(dataset):
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


def create_enriched_stock_view(dataset):
    return f"""
        CREATE OR REPLACE TABLE {dataset}.enriched_stock_data AS (
            SELECT
                stock.id AS stock_id,
                stock.offerId AS offer_id,
                offer.name AS offer_name,
                venue.managingOffererId AS offerer_id,
                offer.type AS offer_type,
                venue.departementCode AS venue_departement_code,
                stock.dateCreated AS stock_issued_at,
                stock.bookingLimitDatetime AS booking_limit_datetime,
                stock.beginningDatetime AS beginning_datetime,
                available_stock_information.stock_disponible_reel,
                stock.quantity AS quantity,
                stock_booking_information.booking_quantity,
                stock_booking_information.bookings_cancelled,
                stock_booking_information.bookings_paid
             FROM {dataset}.stock
             LEFT JOIN {dataset}.offer ON stock.offerId = offer.id
             LEFT JOIN {dataset}.venue ON venue.id = offer.venueId
             LEFT JOIN {dataset}.stock_booking_information ON stock.id = stock_booking_information.stock_id
             LEFT JOIN {dataset}.available_stock_information ON stock_booking_information.stock_id = available_stock_information.stock_id);
    """


def main(dataset):
    client = bigquery.Client()

    overall_query = f"""
            {create_stocks_booking_view(dataset=dataset)}
            {create_available_stocks_view(dataset=dataset)}
            {create_enriched_stock_view(dataset=dataset)}
        """

    run_query(bq_client=client, query=overall_query)


if __name__ == "__main__":
    set_env_vars()
    main(dataset=MIGRATION_ENRICHED_STOCK_DATA)

