import sys

from google.cloud import bigquery

from bigquery.scripts.federated_query.enriched_data_utils import define_humanized_id_query
from bigquery.utils import run_query
from bigquery.config import MIGRATION_ENRICHED_OFFERER_DATA
from set_env import set_env_vars

import logging

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

logger = logging.getLogger()


def define_first_stock_creation_dates_query(dataset):
    return f"""
        CREATE TEMP TABLE related_stocks AS
            SELECT
                offerer.id AS offerer_id,
                MIN(stock.dateCreated) AS date_de_creation_du_premier_stock
            FROM {dataset}.offerer
            LEFT JOIN {dataset}.venue ON venue.managingOffererId = offerer.id
            LEFT JOIN {dataset}.offer ON offer.venueId = venue.id
            LEFT JOIN {dataset}.stock ON stock.offerId = offer.id
            GROUP BY offerer_id;
    """


def define_first_booking_creation_dates_query(dataset):
    return f"""
        CREATE TEMP TABLE related_bookings AS
            SELECT
                offerer.id AS offerer_id,
                MIN(booking.dateCreated) AS date_de_premiere_reservation
            FROM {dataset}.offerer
            LEFT JOIN {dataset}.venue ON venue.managingOffererId = offerer.id
            LEFT JOIN {dataset}.offer ON offer.venueId = venue.id
            LEFT JOIN {dataset}.stock ON stock.offerId = offer.id
            LEFT JOIN {dataset}.booking ON booking.stockId = stock.id
            GROUP BY offerer_id;
        """


def define_number_of_offers_query(dataset):
    return f"""
        CREATE TEMP TABLE related_offers AS
            SELECT
                offerer.id AS offerer_id,
                COUNT(offer.id) AS nombre_offres
            FROM {dataset}.offerer
            LEFT JOIN {dataset}.venue ON venue.managingOffererId = offerer.id
            LEFT JOIN {dataset}.offer ON offer.venueId = venue.id
            GROUP BY offerer_id;
        """


def define_number_of_bookings_not_cancelled_query(dataset):
    return f"""
        CREATE TEMP TABLE related_non_cancelled_bookings AS
            SELECT
                offerer.id AS offerer_id,
                COUNT(booking.id) AS nombre_de_reservations_non_annulees
            FROM {dataset}.offerer
            LEFT JOIN {dataset}.venue ON venue.managingOffererId = offerer.id
            LEFT JOIN {dataset}.offer ON offer.venueId = venue.id
            LEFT JOIN {dataset}.stock ON stock.offerId = offer.id
            LEFT JOIN {dataset}.booking
                ON booking.stockId = stock.id AND booking.isCancelled IS FALSE
            GROUP BY offerer_id;
        """


def define_offerer_departement_code_query(dataset):
    return f"""
        CREATE TEMP TABLE offerer_departement_code AS
            SELECT
                id,
                CASE SUBSTRING(postalCode, 0, 2)
                    WHEN '97' THEN SUBSTRING(postalCode, 0, 3)
                    ELSE SUBSTRING(postalCode, 0, 2)
                END AS department_code
            FROM {dataset}.offerer
            WHERE "postalCode" is not NULL;
    """


def define_number_of_venues_query(dataset):
    return f"""
        CREATE TEMP TABLE related_venues AS
            SELECT
                offerer.id AS offerer_id,
                COUNT(venue.id) AS nombre_lieux
            FROM {dataset}.offerer
            LEFT JOIN {dataset}.venue
            ON offerer.id = venue.managingOffererId
            GROUP BY 1;
        """


def define_number_of_venues_without_offer_query(dataset):
    return f"""
    CREATE TEMP TABLE related_venues_with_offer AS
        WITH venues_with_offers AS (
            SELECT
                offerer.id AS offerer_id,
                venue.id AS venue_id,
                count(offer.id) AS count_offers
            FROM {dataset}.offerer
            LEFT JOIN {dataset}.venue ON offerer.id = venue.managingOffererId
            LEFT JOIN {dataset}.offer ON venue.id = offer.venueId
            GROUP BY offerer_id, venue_id
        )
        SELECT
            offerer_id,
            COUNT(CASE WHEN count_offers > 0 THEN venue_id ELSE NULL END) AS nombre_de_lieux_avec_offres
        FROM venues_with_offers
        GROUP BY offerer_id;
        """


def define_enriched_offerer_query(dataset):
    return f"""
        CREATE OR REPLACE TABLE {dataset}.enriched_offerer_data AS (
            SELECT
                offerer.id AS offerer_id,
                offerer.name AS nom,
                offerer.dateCreated AS date_de_creation,
                related_stocks.date_de_creation_du_premier_stock,
                related_bookings.date_de_premiere_reservation,
                related_offers.nombre_offres,
                related_non_cancelled_bookings.nombre_de_reservations_non_annulees,
                offerer_departement_code.department_code AS departement,
                related_venues.nombre_lieux,
                related_venues_with_offer.nombre_de_lieux_avec_offres,
                offerer_humanized_id.humanized_id AS offerer_humanized_id
            FROM {dataset}.offerer
            LEFT JOIN related_stocks ON related_stocks.offerer_id = offerer.id
            LEFT JOIN related_bookings ON related_bookings.offerer_id = offerer.id
            LEFT JOIN related_offers ON related_offers.offerer_id = offerer.id
            LEFT JOIN related_non_cancelled_bookings
                ON related_non_cancelled_bookings.offerer_id = offerer.id
            LEFT JOIN offerer_departement_code ON offerer_departement_code.id = offerer.id
            LEFT JOIN related_venues ON related_venues.offerer_id = offerer.id
            LEFT JOIN related_venues_with_offer
                ON related_venues_with_offer.offerer_id = offerer.id
            LEFT JOIN offerer_humanized_id ON offerer_humanized_id.id = offerer.id
        );
    """


def main(dataset):
    client = bigquery.Client()

    # Define queries
    first_stock_creation_dates_query = define_first_stock_creation_dates_query(dataset=dataset)
    first_booking_creation_dates_query = define_first_booking_creation_dates_query(dataset=dataset)
    number_of_offers_query = define_number_of_offers_query(dataset=dataset)
    number_of_bookings_not_cancelled_query = define_number_of_bookings_not_cancelled_query(dataset=dataset)
    offerer_departement_code_query = define_offerer_departement_code_query(dataset=dataset)
    number_of_venues_query = define_number_of_venues_query(dataset=dataset)
    number_of_venues_without_offer_query = define_number_of_venues_without_offer_query(dataset=dataset)
    humanized_id_query = define_humanized_id_query(dataset=dataset, table="offerer")
    materialized_enriched_offerer_query = define_enriched_offerer_query(dataset=dataset)

    overall_query = f"""
        {first_stock_creation_dates_query}
        {first_booking_creation_dates_query}
        {number_of_offers_query}
        {number_of_bookings_not_cancelled_query}
        {offerer_departement_code_query}
        {number_of_venues_query}
        {number_of_venues_without_offer_query}
        {humanized_id_query}
        {materialized_enriched_offerer_query}
    """

    # Run queries
    run_query(bq_client=client, query=overall_query)


if __name__ == "__main__":
    set_env_vars()
    main(dataset=MIGRATION_ENRICHED_OFFERER_DATA)
