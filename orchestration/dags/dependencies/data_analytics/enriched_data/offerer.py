from dependencies.data_analytics.enriched_data.enriched_data_utils import (
    define_humanized_id_query,
)


def define_first_stock_creation_dates_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE related_stocks AS
            SELECT
                offerer.id AS offerer_id,
                MIN(stock.dateCreated) AS date_de_creation_du_premier_stock
            FROM {dataset}.{table_prefix}offerer AS offerer
            LEFT JOIN {dataset}.{table_prefix}venue AS venue ON venue.managingOffererId = offerer.id
            LEFT JOIN {dataset}.{table_prefix}offer AS offer ON offer.venueId = venue.id
            LEFT JOIN {dataset}.{table_prefix}stock AS stock ON stock.offerId = offer.id
            GROUP BY offerer_id;
    """


def define_first_booking_creation_dates_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE related_bookings AS
            SELECT
                offerer.id AS offerer_id,
                MIN(booking.dateCreated) AS date_de_premiere_reservation
            FROM {dataset}.{table_prefix}offerer AS offerer
            LEFT JOIN {dataset}.{table_prefix}venue AS venue ON venue.managingOffererId = offerer.id
            LEFT JOIN {dataset}.{table_prefix}offer AS offer ON offer.venueId = venue.id
            LEFT JOIN {dataset}.{table_prefix}stock AS stock ON stock.offerId = offer.id
            LEFT JOIN {dataset}.{table_prefix}booking AS booking ON booking.stockId = stock.id
            GROUP BY offerer_id;
    """


def define_number_of_offers_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE related_offers AS
            SELECT
                offerer.id AS offerer_id,
                COUNT(offer.id) AS nombre_offres
            FROM {dataset}.{table_prefix}offerer AS offerer
            LEFT JOIN {dataset}.{table_prefix}venue AS venue ON venue.managingOffererId = offerer.id
            LEFT JOIN {dataset}.{table_prefix}offer AS offer ON offer.venueId = venue.id
            GROUP BY offerer_id;
    """


def define_number_of_bookings_not_cancelled_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE related_non_cancelled_bookings AS
            SELECT
                offerer.id AS offerer_id,
                COUNT(booking.id) AS nombre_de_reservations_non_annulees
            FROM {dataset}.{table_prefix}offerer AS offerer
            LEFT JOIN {dataset}.{table_prefix}venue AS venue ON venue.managingOffererId = offerer.id
            LEFT JOIN {dataset}.{table_prefix}offer AS offer ON offer.venueId = venue.id
            LEFT JOIN {dataset}.{table_prefix}stock AS stock ON stock.offerId = offer.id
            LEFT JOIN {dataset}.{table_prefix}booking AS booking
                ON booking.stockId = stock.id AND booking.isCancelled IS FALSE
            GROUP BY offerer_id;
    """


def define_offerer_departement_code_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE offerer_departement_code AS
            SELECT
                id,
                CASE SUBSTRING(postalCode, 0, 2)
                    WHEN '97' THEN SUBSTRING(postalCode, 0, 3)
                    ELSE SUBSTRING(postalCode, 0, 2)
                END AS department_code
            FROM {dataset}.{table_prefix}offerer AS offerer
            WHERE "postalCode" is not NULL;
    """


def define_number_of_venues_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE related_venues AS
            SELECT
                offerer.id AS offerer_id,
                COUNT(venue.id) AS nombre_lieux
            FROM {dataset}.{table_prefix}offerer AS offerer
            LEFT JOIN {dataset}.{table_prefix}venue AS venue
            ON offerer.id = venue.managingOffererId
            GROUP BY 1;
    """


def define_number_of_venues_without_offer_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE related_venues_with_offer AS
            WITH venues_with_offers AS (
                SELECT
                    offerer.id AS offerer_id,
                    venue.id AS venue_id,
                    count(offer.id) AS count_offers
                FROM {dataset}.{table_prefix}offerer AS offerer
                LEFT JOIN {dataset}.{table_prefix}venue AS venue ON offerer.id = venue.managingOffererId
                LEFT JOIN {dataset}.{table_prefix}offer AS offer ON venue.id = offer.venueId
                GROUP BY offerer_id, venue_id
            )
            SELECT
                offerer_id,
                COUNT(CASE WHEN count_offers > 0 THEN venue_id ELSE NULL END) AS nombre_de_lieux_avec_offres
            FROM venues_with_offers
            GROUP BY offerer_id;
    """


def define_current_year_revenue(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE current_year_revenue AS
            SELECT
                venue.managingOffererId AS offerer_id,
                sum(coalesce(booking.quantity,0)*coalesce(booking.amount,0)) AS chiffre_affaire_reel_annee_civile_en_cours
            FROM {dataset}.{table_prefix}booking AS booking
            JOIN {dataset}.{table_prefix}stock AS stock ON booking.stockId = stock.id
            JOIN {dataset}.{table_prefix}offer AS offer ON stock.offerId = offer.id
            JOIN {dataset}.{table_prefix}venue AS venue ON offer.venueId = venue.id
            AND EXTRACT(YEAR FROM booking.dateCreated) = EXTRACT(YEAR FROM current_date)
            AND booking.isUsed
            GROUP BY venue.managingOffererId;
    """


def define_enriched_offerer_query(dataset, table_prefix=""):
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
                offerer_humanized_id.humanized_id AS offerer_humanized_id,
                current_year_revenue.chiffre_affaire_reel_annee_civile_en_cours
            FROM {dataset}.{table_prefix}offerer AS offerer
            LEFT JOIN related_stocks ON related_stocks.offerer_id = offerer.id
            LEFT JOIN related_bookings ON related_bookings.offerer_id = offerer.id
            LEFT JOIN related_offers ON related_offers.offerer_id = offerer.id
            LEFT JOIN related_non_cancelled_bookings
                ON related_non_cancelled_bookings.offerer_id = offerer.id
            LEFT JOIN offerer_departement_code ON offerer_departement_code.id = offerer.id
            LEFT JOIN related_venues ON related_venues.offerer_id = offerer.id
            LEFT JOIN related_venues_with_offer
                ON related_venues_with_offer.offerer_id = offerer.id
            LEFT JOIN {table_prefix}offerer_humanized_id AS offerer_humanized_id ON offerer_humanized_id.id = offerer.id
            LEFT JOIN current_year_revenue ON current_year_revenue.offerer_id = offerer.id
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
        {define_humanized_id_query(table=f"{table_prefix}offerer", dataset=dataset)}
        {define_current_year_revenue(dataset=dataset, table_prefix=table_prefix)}
        {define_enriched_offerer_query(dataset=dataset, table_prefix=table_prefix)}
    """
