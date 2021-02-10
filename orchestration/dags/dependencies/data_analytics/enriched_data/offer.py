from dependencies.data_analytics.enriched_data.enriched_data_utils import (
    define_humanized_id_query,
)


def define_is_physical_view_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE is_physical_view AS
            SELECT
                offer.offer_id,
                CASE WHEN offer.offer_type IN ('ThingType.INSTRUMENT',
                                         'ThingType.JEUX',
                                         'ThingType.LIVRE_EDITION',
                                         'ThingType.MUSIQUE',
                                         'ThingType.OEUVRE_ART',
                                         'ThingType.AUDIOVISUEL')
                    AND offer.offer_url IS NULL
                    THEN true
                    ELSE false END as physical_goods
            FROM {dataset}.{table_prefix}offer AS offer;
    """


def define_is_outing_view_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE is_outing_view AS
            SELECT
                offer.offer_id,
                CASE WHEN offer.offer_type IN ('EventType.SPECTACLE_VIVANT',
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
                    THEN true
                    ELSE false END AS outing
            FROM {dataset}.{table_prefix}offer AS offer;
    """


def define_offer_booking_information_view_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE offer_booking_information_view AS
            SELECT
                offer.offer_id,
                SUM(booking.booking_quantity) AS count_booking,
                SUM(CASE WHEN booking.booking_is_cancelled THEN booking.booking_quantity ELSE NULL END)
                    AS count_booking_cancelled,
                SUM(CASE WHEN booking.booking_is_used THEN booking.booking_quantity ELSE NULL END)
                AS count_booking_confirm
            FROM {dataset}.{table_prefix}offer AS offer
            LEFT JOIN {dataset}.{table_prefix}stock AS stock ON stock.offer_id = offer.offer_id
            LEFT JOIN {dataset}.{table_prefix}booking AS booking ON stock.stock_id = booking.stock_id
            GROUP BY offer_id;
    """


def define_count_favorites_view_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE count_favorites_view AS
            SELECT
                offerId,
                COUNT(*) AS count_favorite
            FROM {dataset}.{table_prefix}favorite AS favorite
            GROUP BY offerId;
    """


def define_sum_stock_view_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE sum_stock_view AS
            SELECT
                offer_id,
                SUM(stock_quantity) AS stock
            FROM {dataset}.{table_prefix}stock AS stock
            GROUP BY offer_id;
    """


def define_count_first_booking_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE count_first_booking_view AS
            SELECT
                offer_id, count(*) as first_booking_cnt
            FROM (
                SELECT
                    stock.offer_id,
                    rank() OVER (PARTITION BY booking.user_id ORDER BY booking.booking_creation_date, booking.booking_id)
                    AS booking_rank
                FROM {dataset}.{table_prefix}booking AS booking
                LEFT JOIN {dataset}.{table_prefix}stock AS stock on stock.stock_id = booking.stock_id) c
            WHERE c.booking_rank = 1
            GROUP BY offer_id ORDER BY first_booking_cnt DESC;
    """


def define_enriched_offer_data_query(dataset, table_prefix=""):
    return f"""
        CREATE OR REPLACE TABLE {dataset}.enriched_offer_data AS (
            SELECT
                offerer.offerer_id,
                offerer.offerer_name,
                venue.venue_id,
                venue.venue_name,
                venue.venue_department_code,
                offer.offer_id,
                offer.offer_name,
                offer.offer_type,
                offer.offer_creation_date,
                offer.offer_is_duo,
                venue.venue_is_virtual,
                is_physical_view.physical_goods,
                is_outing_view.outing,
                COALESCE(offer_booking_information_view.count_booking, 0.0) AS booking_cnt,
                COALESCE(offer_booking_information_view.count_booking_cancelled, 0.0)
                    AS booking_cancelled_cnt,
                COALESCE(offer_booking_information_view.count_booking_confirm, 0.0)
                    AS booking_confirm_cnt,
                COALESCE(count_favorites_view.count_favorite, 0.0)
                    AS favourite_cnt,
                COALESCE(sum_stock_view.stock, 0.0) AS stock,
                offer_humanized_id.humanized_id AS offer_humanized_id,
                CONCAT('https://pro.passculture.beta.gouv.fr/offres/', offer_humanized_id.humanized_id)
                    AS passculture_pro_url,
                CONCAT('https://app.passculture.beta.gouv.fr/offre/details/',offer_humanized_id.humanized_id)
                    AS webapp_url,
                count_first_booking_view.first_booking_cnt
            FROM {dataset}.{table_prefix}offer AS offer
            LEFT JOIN {dataset}.{table_prefix}venue AS venue ON offer.venue_id = venue.venue_id
            LEFT JOIN {dataset}.{table_prefix}offerer AS offerer ON venue.venue_managing_offerer_id = offerer.offerer_id
            LEFT JOIN is_physical_view ON is_physical_view.offer_id = offer.offer_id
            LEFT JOIN is_outing_view ON is_outing_view.offer_id = offer.offer_id
            LEFT JOIN offer_booking_information_view ON offer_booking_information_view.offer_id = offer.offer_id
            LEFT JOIN count_favorites_view ON count_favorites_view.offerId = offer.offer_id
            LEFT JOIN sum_stock_view ON sum_stock_view.offer_id = offer.offer_id
            LEFT JOIN offer_humanized_id AS offer_humanized_id ON offer_humanized_id.offer_id = offer.offer_id
            LEFT JOIN count_first_booking_view ON count_first_booking_view.offer_id = offer.offer_id
        );
    """


def define_enriched_offer_data_full_query(dataset, table_prefix=""):
    return f"""
        {define_is_physical_view_query(dataset=dataset, table_prefix=table_prefix)}
        {define_is_outing_view_query(dataset=dataset, table_prefix=table_prefix)}
        {define_offer_booking_information_view_query(dataset=dataset, table_prefix=table_prefix)}
        {define_count_favorites_view_query(dataset=dataset, table_prefix=table_prefix)}
        {define_sum_stock_view_query(dataset=dataset, table_prefix=table_prefix)}
        {define_humanized_id_query(table=f"offer", dataset=dataset, table_prefix=table_prefix)}
        {define_count_first_booking_query(dataset=dataset, table_prefix=table_prefix)}
        {define_enriched_offer_data_query(dataset=dataset, table_prefix=table_prefix)}
    """
