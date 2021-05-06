from dependencies.data_analytics.enriched_data.enriched_data_utils import (
    define_humanized_id_query,
)


def define_total_bookings_per_venue_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE total_bookings_per_venue AS
            SELECT
                venue.venue_id
                ,count(booking.booking_id) AS total_bookings
            FROM {dataset}.{table_prefix}venue AS venue
            LEFT JOIN {dataset}.{table_prefix}offer AS offer
            ON venue.venue_id = offer.venue_id
            AND (offer.booking_email != 'jeux-concours@passculture.app' or offer.booking_email is NULL)
            AND offer.offer_type NOT IN ('EventType.ACTIVATION','ThingType.ACTIVATION')
            LEFT JOIN {dataset}.{table_prefix}stock AS stock
            ON stock.offer_id = offer.offer_id
            LEFT JOIN {dataset}.{table_prefix}booking AS booking
            ON stock.stock_id = booking.stock_id
            GROUP BY venue.venue_id;
    """


def define_non_cancelled_bookings_per_venue_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE non_cancelled_bookings_per_venue AS
            SELECT
                venue.venue_id
                ,count(booking.booking_id) AS non_cancelled_bookings
            FROM {dataset}.{table_prefix}venue AS venue
            LEFT JOIN {dataset}.{table_prefix}offer AS offer
            ON venue.venue_id = offer.venue_id
            AND (offer.booking_email != 'jeux-concours@passculture.app' or offer.booking_email is NULL)
            AND offer.offer_type NOT IN ('EventType.ACTIVATION','ThingType.ACTIVATION')
            LEFT JOIN {dataset}.{table_prefix}stock AS stock
            ON stock.offer_id = offer.offer_id
            LEFT JOIN {dataset}.{table_prefix}booking AS booking
            ON stock.stock_id = booking.stock_id
            AND NOT booking.booking_is_cancelled
            GROUP BY venue.venue_id;
    """


def define_used_bookings_per_venue_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE used_bookings_per_venue AS
            SELECT
                venue.venue_id
                ,count(booking.booking_id) AS used_bookings
            FROM {dataset}.{table_prefix}venue AS venue
            LEFT JOIN {dataset}.{table_prefix}offer AS offer
            ON venue.venue_id = offer.venue_id
            AND (offer.booking_email != 'jeux-concours@passculture.app' or offer.booking_email is NULL)
            AND offer.offer_type NOT IN ('EventType.ACTIVATION','ThingType.ACTIVATION')
            LEFT JOIN {dataset}.{table_prefix}stock AS stock
            ON stock.offer_id = offer.offer_id
            LEFT JOIN {dataset}.{table_prefix}booking AS booking
            ON stock.stock_id = booking.stock_id
            AND  booking.booking_is_used
            GROUP BY venue.venue_id;
    """


def define_first_offer_creation_date_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE first_offer_creation_date AS
            SELECT
                venue.venue_id
                ,MIN(offer.offer_creation_date) AS first_offer_creation_date
            FROM {dataset}.{table_prefix}venue AS venue
            LEFT JOIN {dataset}.{table_prefix}offer AS offer
            ON venue.venue_id = offer.venue_id
            AND (offer.booking_email != 'jeux-concours@passculture.app' or offer.booking_email is NULL)
            AND offer.offer_type NOT IN ('EventType.ACTIVATION','ThingType.ACTIVATION')
            GROUP BY venue.venue_id;
    """


def define_last_offer_creation_date_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE last_offer_creation_date AS
            SELECT
                venue.venue_id
                ,MAX(offer.offer_creation_date) AS last_offer_creation_date
            FROM {dataset}.{table_prefix}venue AS venue
            LEFT JOIN {dataset}.{table_prefix}offer AS offer
            ON venue.venue_id = offer.venue_id
            AND (offer.booking_email != 'jeux-concours@passculture.app' or offer.booking_email is NULL)
            AND offer.offer_type NOT IN ('EventType.ACTIVATION','ThingType.ACTIVATION')
            GROUP BY venue.venue_id;
    """


def define_offers_created_per_venue_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE offers_created_per_venue AS
            SELECT
                venue.venue_id
                ,count(offer.offer_id) AS offers_created
            FROM {dataset}.{table_prefix}venue AS venue
            LEFT JOIN {dataset}.{table_prefix}offer AS offer
            ON venue.venue_id = offer.venue_id
            AND (offer.booking_email != 'jeux-concours@passculture.app' or offer.booking_email is NULL)
            AND offer.offer_type NOT IN ('EventType.ACTIVATION','ThingType.ACTIVATION')
            GROUP BY venue.venue_id;
    """


def define_theoretic_revenue_per_venue_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE theoretic_revenue_per_venue AS
            SELECT
                venue.venue_id
                ,COALESCE(SUM(booking.booking_amount * booking.booking_quantity), 0) AS theoretic_revenue
            FROM {dataset}.{table_prefix}venue AS venue
            LEFT JOIN {dataset}.{table_prefix}offer AS offer
            ON venue.venue_id = offer.venue_id
            AND (offer.booking_email != 'jeux-concours@passculture.app' or offer.booking_email is NULL)
            AND offer.offer_type NOT IN ('EventType.ACTIVATION','ThingType.ACTIVATION')
            LEFT JOIN {dataset}.{table_prefix}stock AS stock
            ON offer.offer_id = stock.offer_id
            LEFT JOIN {dataset}.{table_prefix}booking AS booking
            ON booking.stock_id = stock.stock_id
            AND NOT booking.booking_is_cancelled
            GROUP BY venue.venue_id;
    """


def define_real_revenue_per_venue_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE real_revenue_per_venue AS
        SELECT
                venue.venue_id
                ,COALESCE(SUM(booking.booking_amount * booking.booking_quantity), 0) AS real_revenue
            FROM {dataset}.{table_prefix}venue AS venue
            LEFT JOIN {dataset}.{table_prefix}offer AS offer
            ON venue.venue_id = offer.venue_id
            AND (offer.booking_email != 'jeux-concours@passculture.app' or offer.booking_email is NULL)
            AND offer.offer_type NOT IN ('EventType.ACTIVATION','ThingType.ACTIVATION')
            LEFT JOIN {dataset}.{table_prefix}stock AS stock
            ON offer.offer_id = stock.offer_id
            LEFT JOIN {dataset}.{table_prefix}booking AS booking
            ON booking.stock_id = stock.stock_id
            AND NOT booking.booking_is_cancelled
            AND booking.booking_is_used
            GROUP BY venue.venue_id;
    """


def define_enriched_venue_query(dataset, table_prefix=""):
    return f"""
        CREATE OR REPLACE TABLE {dataset}.enriched_venue_data AS (
            SELECT
                venue.venue_id
                ,COALESCE(NULLIF(venue.venue_public_name, ""), venue.venue_name) AS venue_name
                ,venue.venue_booking_email
                ,venue.venue_address
                ,venue.venue_latitude
                ,venue.venue_longitude
                ,venue.venue_department_code
                ,venue.venue_postal_code
                ,venue.venue_city
                ,venue.venue_siret
                ,venue.venue_is_virtual
                ,venue.venue_managing_offerer_id
                ,venue.venue_creation_date
                ,offerer.offerer_name
                ,venue_type.label AS venue_type_label
                ,venue_label.label AS venue_label
                ,total_bookings_per_venue.total_bookings
                ,non_cancelled_bookings_per_venue.non_cancelled_bookings
                ,used_bookings_per_venue.used_bookings
                ,first_offer_creation_date.first_offer_creation_date
                ,last_offer_creation_date.last_offer_creation_date
                ,offers_created_per_venue.offers_created
                ,theoretic_revenue_per_venue.theoretic_revenue
                ,real_revenue_per_venue.real_revenue
                ,venue_humanized_id.humanized_id AS venue_humanized_id
                ,CONCAT("https://backend.passculture.beta.gouv.fr/pc/back-office/venue/edit/?id=",venue.venue_id,"&url=%2Fpc%2Fback-office%2Fvenue%2F") AS venue_flaskadmin_link
                ,venue_region_departement.region_name AS venue_region_name
            FROM {dataset}.{table_prefix}venue AS venue
            LEFT JOIN {dataset}.{table_prefix}offerer AS offerer ON venue.venue_managing_offerer_id = offerer.offerer_id
            LEFT JOIN {dataset}.{table_prefix}venue_type AS venue_type ON venue.venue_type_id = venue_type.id
            LEFT JOIN {dataset}.{table_prefix}venue_label AS venue_label ON venue_label.id = venue.venue_label_id
            LEFT JOIN total_bookings_per_venue ON venue.venue_id = total_bookings_per_venue.venue_id
            LEFT JOIN non_cancelled_bookings_per_venue ON venue.venue_id = non_cancelled_bookings_per_venue.venue_id
            LEFT JOIN used_bookings_per_venue ON venue.venue_id = used_bookings_per_venue.venue_id
            LEFT JOIN first_offer_creation_date ON venue.venue_id = first_offer_creation_date.venue_id
            LEFT JOIN last_offer_creation_date ON venue.venue_id = last_offer_creation_date.venue_id
            LEFT JOIN offers_created_per_venue ON venue.venue_id = offers_created_per_venue.venue_id
            LEFT JOIN theoretic_revenue_per_venue ON venue.venue_id = theoretic_revenue_per_venue.venue_id
            LEFT JOIN real_revenue_per_venue ON venue.venue_id = real_revenue_per_venue.venue_id
            LEFT JOIN venue_humanized_id AS venue_humanized_id ON venue_humanized_id.venue_id = venue.venue_id
            LEFT JOIN {dataset}.region_department AS venue_region_departement ON venue.venue_department_code = venue_region_departement.num_dep
        );
    """


def define_enriched_venue_data_full_query(dataset, table_prefix=""):
    return f"""
        {define_total_bookings_per_venue_query(dataset=dataset, table_prefix=table_prefix)}
        {define_non_cancelled_bookings_per_venue_query(dataset=dataset, table_prefix=table_prefix)}
        {define_used_bookings_per_venue_query(dataset=dataset, table_prefix=table_prefix)}
        {define_first_offer_creation_date_query(dataset=dataset, table_prefix=table_prefix)}
        {define_last_offer_creation_date_query(dataset=dataset, table_prefix=table_prefix)}
        {define_offers_created_per_venue_query(dataset=dataset, table_prefix=table_prefix)}
        {define_theoretic_revenue_per_venue_query(dataset=dataset, table_prefix=table_prefix)}
        {define_real_revenue_per_venue_query(dataset=dataset, table_prefix=table_prefix)}
        {define_humanized_id_query(table=f"venue", dataset=dataset, table_prefix=table_prefix)}
        {define_enriched_venue_query(dataset=dataset, table_prefix=table_prefix)}
    """
