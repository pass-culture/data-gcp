from dependencies.data_analytics.enriched_data.enriched_data_utils import (
    define_humanized_id_query,
)


def define_first_stock_creation_dates_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE related_stocks AS
            SELECT
                offerer.offerer_id,
                MIN(stock.stock_creation_date) AS first_stock_creation_date
            FROM {dataset}.{table_prefix}offerer AS offerer
            LEFT JOIN {dataset}.{table_prefix}venue AS venue ON venue.venue_managing_offerer_id = offerer.offerer_id
            LEFT JOIN {dataset}.{table_prefix}offer AS offer ON offer.venue_id = venue.venue_id
            LEFT JOIN {dataset}.{table_prefix}stock AS stock ON stock.offer_id = offer.offer_id
            GROUP BY offerer_id;
    """


def define_first_booking_creation_dates_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE related_bookings AS
            SELECT
                offerer.offerer_id,
                MIN(booking.booking_creation_date) AS first_booking_date
            FROM {dataset}.{table_prefix}offerer AS offerer
            LEFT JOIN {dataset}.{table_prefix}venue AS venue ON venue.venue_managing_offerer_id = offerer.offerer_id
            LEFT JOIN {dataset}.{table_prefix}offer AS offer ON offer.venue_id = venue.venue_id
            LEFT JOIN {dataset}.{table_prefix}stock AS stock ON stock.offer_id = offer.offer_id
            LEFT JOIN {dataset}.{table_prefix}booking AS booking ON booking.stock_id = stock.stock_id
            GROUP BY offerer_id;
    """


def define_number_of_offers_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE related_offers AS
            SELECT
                offerer.offerer_id,
                COUNT(offer.offer_id) AS offer_cnt
            FROM {dataset}.{table_prefix}offerer AS offerer
            LEFT JOIN {dataset}.{table_prefix}venue AS venue ON venue.venue_managing_offerer_id = offerer.offerer_id
            LEFT JOIN {dataset}.{table_prefix}offer AS offer ON offer.venue_id = venue.venue_id
            GROUP BY offerer_id;
    """


def define_number_of_bookings_not_cancelled_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE related_non_cancelled_bookings AS
            SELECT
                offerer.offerer_id,
                COUNT(booking.booking_id) AS no_cancelled_booking_cnt
            FROM {dataset}.{table_prefix}offerer AS offerer
            LEFT JOIN {dataset}.{table_prefix}venue AS venue ON venue.venue_managing_offerer_id = offerer.offerer_id
            LEFT JOIN {dataset}.{table_prefix}offer AS offer ON offer.venue_id = venue.venue_id
            LEFT JOIN {dataset}.{table_prefix}stock AS stock ON stock.offer_id = offer.offer_id
            LEFT JOIN {dataset}.{table_prefix}booking AS booking
                ON booking.stock_id = stock.stock_id AND booking.booking_is_cancelled IS FALSE
            GROUP BY offerer_id;
    """


def define_offerer_departement_code_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE offerer_department_code AS
            SELECT
                offerer.offerer_id,
                CASE SUBSTRING(offerer_postal_code, 0, 2)
                    WHEN '97' THEN SUBSTRING(offerer_postal_code, 0, 3)
                    ELSE SUBSTRING(offerer_postal_code, 0, 2)
                END AS offerer_department_code
            FROM {dataset}.{table_prefix}offerer AS offerer
            WHERE "offerer_postal_code" is not NULL;
    """


def define_number_of_venues_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE related_venues AS
            SELECT
                offerer.offerer_id,
                COUNT(venue.venue_id) AS venue_cnt
            FROM {dataset}.{table_prefix}offerer AS offerer
            LEFT JOIN {dataset}.{table_prefix}venue AS venue
            ON offerer.offerer_id = venue.venue_managing_offerer_id
            GROUP BY 1;
    """


def define_number_of_venues_without_offer_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE related_venues_with_offer AS
            WITH venues_with_offers AS (
                SELECT
                    offerer.offerer_id,
                    venue.venue_id,
                    count(offer.offer_id) AS count_offers
                FROM {dataset}.{table_prefix}offerer AS offerer
                LEFT JOIN {dataset}.{table_prefix}venue AS venue ON offerer.offerer_id = venue.venue_managing_offerer_id
                LEFT JOIN {dataset}.{table_prefix}offer AS offer ON venue.venue_id = offer.venue_id
                GROUP BY offerer_id, venue_id
            )
            SELECT
                offerer_id,
                COUNT(CASE WHEN count_offers > 0 THEN venue_id ELSE NULL END) AS venue_with_offer
            FROM venues_with_offers
            GROUP BY offerer_id;
    """


def define_current_year_revenue(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE current_year_revenue AS
            SELECT
                venue.venue_managing_offerer_id AS offerer_id,
                sum(coalesce(booking.booking_quantity,0)*coalesce(booking.booking_amount,0)) 
                AS current_year_revenue
            FROM {dataset}.{table_prefix}booking AS booking
            JOIN {dataset}.{table_prefix}stock AS stock ON booking.stock_id = stock.stock_id
            JOIN {dataset}.{table_prefix}offer AS offer ON stock.offer_id = offer.offer_id
            JOIN {dataset}.{table_prefix}venue AS venue ON offer.venue_id = venue.venue_id
            AND EXTRACT(YEAR FROM booking.booking_creation_date) = EXTRACT(YEAR FROM current_date)
            AND booking.booking_is_used
            GROUP BY venue.venue_managing_offerer_id;
    """


def define_enriched_offerer_query(dataset, table_prefix=""):
    return f"""
        CREATE OR REPLACE TABLE {dataset}.enriched_offerer_data AS (
            SELECT
                offerer.offerer_id,
                offerer.offerer_name,
                offerer.offerer_creation_date,
                related_stocks.first_stock_creation_date,
                related_bookings.first_booking_date,
                related_offers.offer_cnt,
                related_non_cancelled_bookings.no_cancelled_booking_cnt,
                offerer_department_code.offerer_department_code,
                related_venues.venue_cnt,
                related_venues_with_offer.venue_with_offer,
                offerer_humanized_id.humanized_id AS offerer_humanized_id,
                current_year_revenue.current_year_revenue
            FROM {dataset}.{table_prefix}offerer AS offerer
            LEFT JOIN related_stocks ON related_stocks.offerer_id = offerer.offerer_id
            LEFT JOIN related_bookings ON related_bookings.offerer_id = offerer.offerer_id
            LEFT JOIN related_offers ON related_offers.offerer_id = offerer.offerer_id
            LEFT JOIN related_non_cancelled_bookings
                ON related_non_cancelled_bookings.offerer_id = offerer.offerer_id
            LEFT JOIN offerer_department_code ON offerer_department_code.offerer_id = offerer.offerer_id
            LEFT JOIN related_venues ON related_venues.offerer_id = offerer.offerer_id
            LEFT JOIN related_venues_with_offer
                ON related_venues_with_offer.offerer_id = offerer.offerer_id
            LEFT JOIN offerer_humanized_id ON offerer_humanized_id.offerer_id = offerer.offerer_id
            LEFT JOIN current_year_revenue ON current_year_revenue.offerer_id = offerer.offerer_id
        );
    """


def define_enriched_offerer_data_full_query(dataset, table_prefix=""):
    return f"""
        {define_first_stock_creation_dates_query(dataset=dataset, table_prefix=table_prefix)}
        {define_first_booking_creation_dates_query(dataset=dataset, table_prefix=table_prefix)}
        {define_number_of_offers_query(dataset=dataset, table_prefix=table_prefix)}
        {define_number_of_bookings_not_cancelled_query(dataset=dataset, table_prefix=table_prefix)}
        {define_offerer_departement_code_query(dataset=dataset, table_prefix=table_prefix)}
        {define_number_of_venues_query(dataset=dataset, table_prefix=table_prefix)}
        {define_number_of_venues_without_offer_query(dataset=dataset, table_prefix=table_prefix)}
        {define_humanized_id_query(table=f"offerer", dataset=dataset)}
        {define_current_year_revenue(dataset=dataset, table_prefix=table_prefix)}
        {define_enriched_offerer_query(dataset=dataset, table_prefix=table_prefix)}
    """
