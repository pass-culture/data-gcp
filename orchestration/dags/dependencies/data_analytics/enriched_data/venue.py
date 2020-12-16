from dependencies.data_analytics.enriched_data.enriched_data_utils import (
    define_humanized_id_query,
)


def define_total_bookings_per_venue_query(dataset):
    return f"""
        CREATE TEMP TABLE total_bookings_per_venue AS
            SELECT
                venue.id AS venue_id
                ,count(booking.id) AS total_bookings
            FROM {dataset}.venue
            LEFT JOIN {dataset}.offer
            ON venue.id = offer.venueId
            AND (offer.bookingEmail != 'jeux-concours@passculture.app' or offer.bookingEmail is NULL)
            AND offer.type NOT IN ('EventType.ACTIVATION','ThingType.ACTIVATION')
            LEFT JOIN {dataset}.stock
            ON stock.offerId = offer.id
            LEFT JOIN {dataset}.booking
            ON stock.id = booking.stockId
            GROUP BY venue.id;
    """


def define_non_cancelled_bookings_per_venue_query(dataset):
    return f"""
        CREATE TEMP TABLE non_cancelled_bookings_per_venue AS
            SELECT
                venue.id AS venue_id
                ,count(booking.id) AS non_cancelled_bookings
            FROM {dataset}.venue
            LEFT JOIN {dataset}.offer
            ON venue.id = offer.venueId
            AND (offer.bookingEmail != 'jeux-concours@passculture.app' or offer.bookingEmail is NULL)
            AND offer.type NOT IN ('EventType.ACTIVATION','ThingType.ACTIVATION')
            LEFT JOIN {dataset}.stock
            ON stock.offerId = offer.id
            LEFT JOIN {dataset}.booking
            ON stock.id = booking.stockId
            AND NOT booking.isCancelled
            GROUP BY venue.id;
    """


def define_used_bookings_per_venue_query(dataset):
    return f"""
        CREATE TEMP TABLE used_bookings_per_venue AS
            SELECT
                venue.id AS venue_id
                ,count(booking.id) AS used_bookings
            FROM {dataset}.venue
            LEFT JOIN {dataset}.offer
            ON venue.id = offer.venueId
            AND (offer.bookingEmail != 'jeux-concours@passculture.app' or offer.bookingEmail is NULL)
            AND offer.type NOT IN ('EventType.ACTIVATION','ThingType.ACTIVATION')
            LEFT JOIN {dataset}.stock
            ON stock.offerId = offer.id
            LEFT JOIN {dataset}.booking
            ON stock.id = booking.stockId
            AND  booking.isUsed
            GROUP BY venue.id;
    """


def define_first_offer_creation_date_query(dataset):
    return f"""
        CREATE TEMP TABLE first_offer_creation_date AS
            SELECT
                venue.id AS venue_id
                ,MIN(offer.dateCreated) AS first_offer_creation_date
            FROM {dataset}.venue
            LEFT JOIN {dataset}.offer
            ON venue.id = offer.venueId
            AND (offer.bookingEmail != 'jeux-concours@passculture.app' or offer.bookingEmail is NULL)
            AND offer.type NOT IN ('EventType.ACTIVATION','ThingType.ACTIVATION')
            GROUP BY venue.id;
    """


def define_last_offer_creation_date_query(dataset):
    return f"""
        CREATE TEMP TABLE last_offer_creation_date AS
            SELECT
                venue.id AS venue_id
                ,MAX(offer.dateCreated) AS last_offer_creation_date
            FROM {dataset}.venue
            LEFT JOIN {dataset}.offer
            ON venue.id = offer.venueId
            AND (offer.bookingEmail != 'jeux-concours@passculture.app' or offer.bookingEmail is NULL)
            AND offer.type NOT IN ('EventType.ACTIVATION','ThingType.ACTIVATION')
            GROUP BY venue.id;
    """


def define_offers_created_per_venue_query(dataset):
    return f"""
        CREATE TEMP TABLE offers_created_per_venue AS
            SELECT
                venue.id AS venue_id
                ,count(offer.id) AS offers_created
            FROM {dataset}.venue
            LEFT JOIN {dataset}.offer
            ON venue.id = offer.venueId
            AND (offer.bookingEmail != 'jeux-concours@passculture.app' or offer.bookingEmail is NULL)
            AND offer.type NOT IN ('EventType.ACTIVATION','ThingType.ACTIVATION')
            GROUP BY venue.id;
    """


def define_theoretic_revenue_per_venue_query(dataset):
    return f"""
        CREATE TEMP TABLE theoretic_revenue_per_venue AS
            SELECT
                venue.id AS venue_id
                ,COALESCE(SUM(booking.amount * booking.quantity), 0) AS theoretic_revenue
            FROM {dataset}.venue
            LEFT JOIN {dataset}.offer
            ON venue.id = offer.venueId
            AND (offer.bookingEmail != 'jeux-concours@passculture.app' or offer.bookingEmail is NULL)
            AND offer.type NOT IN ('EventType.ACTIVATION','ThingType.ACTIVATION')
            LEFT JOIN {dataset}.stock
            ON offer.id = stock.offerId
            LEFT JOIN {dataset}.booking
            ON booking.stockId = stock.id
            AND NOT booking.isCancelled
            GROUP BY venue.id;
    """


def define_real_revenue_per_venue_query(dataset):
    return f"""
        CREATE TEMP TABLE real_revenue_per_venue AS
        SELECT
                venue.id AS venue_id
                ,COALESCE(SUM(booking.amount * booking.quantity), 0) AS real_revenue
            FROM {dataset}.venue
            LEFT JOIN {dataset}.offer
            ON venue.id = offer.venueId
            AND (offer.bookingEmail != 'jeux-concours@passculture.app' or offer.bookingEmail is NULL)
            AND offer.type NOT IN ('EventType.ACTIVATION','ThingType.ACTIVATION')
            LEFT JOIN {dataset}.stock
            ON offer.id = stock.offerId
            LEFT JOIN {dataset}.booking
            ON booking.stockId = stock.id
            AND NOT booking.isCancelled
            AND booking.isUsed
            GROUP BY venue.id;
    """


def define_enriched_venue_query(dataset):
    return f"""
        CREATE OR REPLACE TABLE {dataset}.enriched_venue_data AS (
            SELECT
                venue.id AS venue_id
                ,COALESCE(venue.publicName,venue.name) AS nom_du_lieu
                ,venue.bookingEmail AS email
                ,venue.address AS adresse
                ,venue.latitude
                ,venue.longitude
                ,venue.departementCode AS departement
                ,venue.postalCode AS code_postal
                ,venue.city AS ville
                ,venue.siret
                ,venue.isVirtual AS lieu_numerique
                ,venue.managingOffererId AS identifiant_de_la_structure
                ,offerer.name AS nom_de_la_structure
                ,venue_type.label AS type_de_lieu
                ,venue_label.label AS label_du_lieu
                ,total_bookings_per_venue.total_bookings AS nombre_total_de_reservations
                ,non_cancelled_bookings_per_venue.non_cancelled_bookings AS nombre_de_reservations_non_annulees
                ,used_bookings_per_venue.used_bookings AS nombre_de_reservations_validees
                ,first_offer_creation_date.first_offer_creation_date AS date_de_creation_de_la_premiere_offre
                ,last_offer_creation_date.last_offer_creation_date AS date_de_creation_de_la_derniere_offre
                ,offers_created_per_venue.offers_created AS nombre_offres_creees
                ,theoretic_revenue_per_venue.theoretic_revenue AS chiffre_affaires_theorique_realise
                ,real_revenue_per_venue.real_revenue AS chiffre_affaires_reel_realise
                ,venue_humanized_id.humanized_id AS venue_humanized_id
            FROM {dataset}.venue
            LEFT JOIN {dataset}.offerer ON venue.managingOffererId = offerer.id
            LEFT JOIN {dataset}.venue_type ON venue.venueTypeId = venue_type.id
            LEFT JOIN {dataset}.venue_label ON venue_label.id = venue.venueLabelId
            LEFT JOIN total_bookings_per_venue ON venue.id = total_bookings_per_venue.venue_id
            LEFT JOIN non_cancelled_bookings_per_venue ON venue.id = non_cancelled_bookings_per_venue.venue_id
            LEFT JOIN used_bookings_per_venue ON venue.id = used_bookings_per_venue.venue_id
            LEFT JOIN first_offer_creation_date ON venue.id = first_offer_creation_date.venue_id
            LEFT JOIN last_offer_creation_date ON venue.id = last_offer_creation_date.venue_id
            LEFT JOIN offers_created_per_venue ON venue.id = offers_created_per_venue.venue_id
            LEFT JOIN theoretic_revenue_per_venue ON venue.id = theoretic_revenue_per_venue.venue_id
            LEFT JOIN real_revenue_per_venue ON venue.id = real_revenue_per_venue.venue_id
            LEFT JOIN venue_humanized_id ON venue_humanized_id.id = venue.id
        );
    """


def define_enriched_venue_data_full_query(dataset):
    return f"""
        {define_total_bookings_per_venue_query(dataset=dataset)}
        {define_non_cancelled_bookings_per_venue_query(dataset=dataset)}
        {define_used_bookings_per_venue_query(dataset=dataset)}
        {define_first_offer_creation_date_query(dataset=dataset)}
        {define_last_offer_creation_date_query(dataset=dataset)}
        {define_offers_created_per_venue_query(dataset=dataset)}
        {define_theoretic_revenue_per_venue_query(dataset=dataset)}
        {define_real_revenue_per_venue_query(dataset=dataset)}
        {define_humanized_id_query(table="venue", dataset=dataset)}
        {define_enriched_venue_query(dataset=dataset)}
    """
