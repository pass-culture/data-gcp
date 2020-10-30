import sys

from google.cloud import bigquery

from bigquery.utils import run_query
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
        CREATE  TEMP TABLE related_non_cancelled_bookings AS
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


# def define_offerer_departement_code_query():
#     MAINLAND_DEPARTEMENT_CODE_LENGTH = 2
#     OVERSEAS_DEPARTEMENT_CODE_LENGTH = 3
#     OVERSEAS_DEPARTEMENT_IDENTIFIER = "97"
#
#     def _is_overseas_departement(postalCode: str) -> bool:
#         return postalCode.startswith(OVERSEAS_DEPARTEMENT_IDENTIFIER)
#
#     def get_departement_code(postalCode: str) -> str:
#         return (
#             postalCode[:OVERSEAS_DEPARTEMENT_CODE_LENGTH]
#             if _is_overseas_departement(postalCode)
#             else postalCode[:MAINLAND_DEPARTEMENT_CODE_LENGTH]
#         )
#
#     def get_offerer_with_departement_code_dataframe(postal_code_dataframe: pandas.DataFrame) -> pandas.DataFrame:
#         department_code_dataframe = postal_code_dataframe.copy()
#         department_code_dataframe["department_code"] = department_code_dataframe[
#             "postalCode"
#         ].apply(get_departement_code)
#         return department_code_dataframe.drop("postalCode", axis=1)
#
#     def create_table_offerer_departement_code(department_code_dataframe: pandas.DataFrame, ENGINE) -> None:
#         with ENGINE.connect() as connection:
#             department_code_dataframe.to_sql(
#                 name="offerer_departement_code",
#                 con=connection,
#                 if_exists="replace",
#                 dtype={
#                     "id": sqlalchemy.types.BIGINT(),
#                     "APE_label": sqlalchemy.types.VARCHAR(length=250),
#                 },
#             )
#
#     query = """
#     SELECT
#         id
#         ,"postalCode"
#     FROM offerer
#     WHERE "postalCode" is not NULL
#     """
#     postal_code_dataframe = to_pandas(query)
#
#     department_code_dataframe = get_offerer_with_departement_code_dataframe(
#         postal_code_dataframe
#     )
#     create_table_offerer_departement_code(department_code_dataframe, ENGINE)
#     return ""


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


# def define_humanized_id_query():
#     def int_to_bytes(x):
#         return x.to_bytes((x.bit_length() + 7) // 8, "big")
#
#     def humanize(integer):
#         """ Create a human-compatible ID from and integer """
#         if integer is None:
#             return None
#         b32 = b32encode(int_to_bytes(integer))
#         return b32.decode("ascii").replace("O", "8").replace("I", "9").rstrip("=")
#
#     def get_humanized_id_dataframe(id_dataframe: pandas.DataFrame) -> pandas.DataFrame:
#         humanized_id_dataframe = id_dataframe.copy()
#         humanized_id_dataframe["humanized_id"] = humanized_id_dataframe["id"].apply(
#             humanize
#         )
#         return humanized_id_dataframe
#
#     def create_table_humanized_id(ENGINE, table_name: str, humanized_id_dataframe: pandas.DataFrame) -> None:
#         with ENGINE.connect() as connection:
#             humanized_id_dataframe.to_sql(
#                 name="user_humanized_id"
#                 if table_name == '"user"'
#                 else "{}_humanized_id".format(table_name),
#                 con=connection,
#                 if_exists="replace",
#                 method="multi",
#                 chunksize=500,
#                 dtype={
#                     "id": sqlalchemy.types.BIGINT(),
#                     "humanized_id": sqlalchemy.types.VARCHAR(length=250),
#                 },
#             )
#
#     query = """
#             SELECT id
#             FROM offerer
#             WHERE id is not NULL
#         """
#     id_dataframe = to_pandas(query)
#     humanized_id_dataframe = get_humanized_id_dataframe(id_dataframe)
#     create_table_humanized_id(ENGINE, "offerer", humanized_id_dataframe)
#     return ""


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
                -- offerer_departement_code.department_code AS departement,
                related_venues.nombre_lieux,
                related_venues_with_offer.nombre_de_lieux_avec_offres,
                -- offerer_humanized_id.humanized_id AS offerer_humanized_id
            FROM {dataset}.offerer
            LEFT JOIN related_stocks ON related_stocks.offerer_id = offerer.id
            LEFT JOIN related_bookings ON related_bookings.offerer_id = offerer.id
            LEFT JOIN related_offers ON related_offers.offerer_id = offerer.id
            LEFT JOIN related_non_cancelled_bookings 
                ON related_non_cancelled_bookings.offerer_id = offerer.id
            -- LEFT JOIN offerer_departement_code ON offerer_departement_code.id = offerer.id
            LEFT JOIN related_venues ON related_venues.offerer_id = offerer.id
            LEFT JOIN related_venues_with_offer 
                ON related_venues_with_offer.offerer_id = offerer.id
            -- LEFT JOIN offerer_humanized_id ON offerer_humanized_id.id = offerer.id
        );
    """


def main(dataset):
    client = bigquery.Client()

    # Define queries
    first_stock_creation_dates_query = define_first_stock_creation_dates_query(dataset=dataset)
    first_booking_creation_dates_query = define_first_booking_creation_dates_query(dataset=dataset)
    number_of_offers_query = define_number_of_offers_query(dataset=dataset)
    number_of_bookings_not_cancelled_query = define_number_of_bookings_not_cancelled_query(dataset=dataset)
    # offerer_departement_code_query = define_offerer_departement_code_query()
    number_of_venues_query = define_number_of_venues_query(dataset=dataset)
    number_of_venues_without_offer_query = define_number_of_venues_without_offer_query(dataset=dataset)
    # humanized_id_query = define_humanized_id_query()
    materialized_enriched_offerer_query = define_enriched_offerer_query(dataset=dataset)

    overall_query = f"""
        {first_stock_creation_dates_query}
        {first_booking_creation_dates_query}
        {number_of_offers_query}
        {number_of_bookings_not_cancelled_query}
        {number_of_venues_query}
        {number_of_venues_without_offer_query}
        {materialized_enriched_offerer_query}
    """

    # Run queries
    run_query(bq_client=client, query=overall_query)


if __name__ == "__main__":
    set_env_vars()
    main(dataset="migration_enriched_offerer_data")
