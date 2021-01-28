from dependencies.data_analytics.enriched_data.enriched_data_utils import (
    define_humanized_id_query,
)


def define_is_physical_view_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE is_physical_view AS 
            SELECT
                offer.id AS offer_id,
                CASE WHEN offer.type IN ('ThingType.INSTRUMENT',
                                         'ThingType.JEUX',
                                         'ThingType.LIVRE_EDITION',
                                         'ThingType.MUSIQUE',
                                         'ThingType.OEUVRE_ART',
                                         'ThingType.AUDIOVISUEL')
                    AND offer.url IS NULL
                    THEN true 
                    ELSE false END as bien_physique
            FROM {dataset}.{table_prefix}offer AS offer;
    """


def define_is_outing_view_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE is_outing_view AS 
            SELECT
                offer.id AS offer_id,
                CASE WHEN offer.type IN ('EventType.SPECTACLE_VIVANT',
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
                    ELSE false END AS sortie
            FROM {dataset}.{table_prefix}offer AS offer;
    """


def define_offer_booking_information_view_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE offer_booking_information_view AS 
            SELECT
                offer.id AS offer_id,
                SUM(booking.quantity) AS nombre_reservations,
                SUM(CASE WHEN booking.isCancelled THEN booking.quantity ELSE NULL END) 
                    AS nombre_reservations_annulees,
                SUM(CASE WHEN booking.isUsed THEN booking.quantity ELSE NULL END) AS nombre_reservations_validees
            FROM {dataset}.{table_prefix}offer AS offer
            LEFT JOIN {dataset}.{table_prefix}stock AS stock ON stock.offerId = offer.id
            LEFT JOIN {dataset}.{table_prefix}booking AS booking ON stock.id = booking.stockId
            GROUP BY offer_id;
    """


def define_count_favorites_view_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE count_favorites_view AS
            SELECT
                offerId AS offer_id,
                COUNT(*) AS nombre_fois_ou_l_offre_a_ete_mise_en_favoris
            FROM {dataset}.{table_prefix}favorite AS favorite
            GROUP BY offer_id;
    """


def define_sum_stock_view_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE sum_stock_view AS
            SELECT
                offerId AS offer_id,
                SUM(quantity) AS stock
            FROM {dataset}.{table_prefix}stock AS stock
            GROUP BY offer_id;
    """


def define_count_first_booking_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE count_first_booking_view AS
            SELECT 
                offer_id, count(*) as nombre_de_premieres_reservations 
            FROM (
                SELECT 
                    stock.offerId as offer_id,
                    rank() OVER (PARTITION BY booking.userId ORDER BY booking.dateCreated, booking.id) AS classement_de_la_reservation
                FROM {dataset}.{table_prefix}booking AS booking
                LEFT JOIN {dataset}.{table_prefix}stock AS stock on stock.id = booking.stockId) c 
            WHERE c.classement_de_la_reservation = 1
            GROUP BY offer_id ORDER BY nombre_de_premieres_reservations DESC;
    """


def define_enriched_offer_data_query(dataset, table_prefix=""):
    return f"""
        CREATE OR REPLACE TABLE {dataset}.enriched_offer_data AS (
            SELECT
                offerer.id AS identifiant_structure,
                offerer.name AS nom_structure,
                venue.id AS identifiant_lieu,
                venue.name AS nom_lieu,
                venue.departementCode AS departement_lieu,
                offer.id AS offer_id,
                offer.name AS nom_offre,
                offer.type AS categorie_offre,
                offer.dateCreated AS date_creation_offre,
                offer.isDuo AS duo,
                venue.isVirtual AS offre_numerique,
                is_physical_view.bien_physique,
                is_outing_view.sortie,
                COALESCE(offer_booking_information_view.nombre_reservations, 0.0) AS nombre_reservations,
                COALESCE(offer_booking_information_view.nombre_reservations_annulees, 0.0) 
                    AS nombre_reservations_annulees,
                COALESCE(offer_booking_information_view.nombre_reservations_validees, 0.0) 
                    AS nombre_reservations_validees,
                COALESCE(count_favorites_view.nombre_fois_ou_l_offre_a_ete_mise_en_favoris, 0.0) 
                    AS nombre_fois_ou_l_offre_a_ete_mise_en_favoris,
                COALESCE(sum_stock_view.stock, 0.0) AS stock,
                offer_humanized_id.humanized_id AS offer_humanized_id,
                CONCAT('https://pro.passculture.beta.gouv.fr/offres/', offer_humanized_id.humanized_id) 
                    AS lien_portail_pro,
                CONCAT('https://app.passculture.beta.gouv.fr/offre/details/',offer_humanized_id.humanized_id) 
                    AS lien_webapp, 
                count_first_booking_view.nombre_de_premieres_reservations
            FROM {dataset}.{table_prefix}offer AS offer
            LEFT JOIN {dataset}.{table_prefix}venue AS venue ON offer.venueId = venue.id
            LEFT JOIN {dataset}.{table_prefix}offerer AS offerer ON venue.managingOffererId = offerer.id
            LEFT JOIN is_physical_view ON is_physical_view.offer_id = offer.id
            LEFT JOIN is_outing_view ON is_outing_view.offer_id = offer.id
            LEFT JOIN offer_booking_information_view ON offer_booking_information_view.offer_id = offer.id
            LEFT JOIN count_favorites_view ON count_favorites_view.offer_id = offer.id
            LEFT JOIN sum_stock_view ON sum_stock_view.offer_id = offer.id
            LEFT JOIN {table_prefix}offer_humanized_id AS offer_humanized_id ON offer_humanized_id.id = offer.id
            LEFT JOIN count_first_booking_view ON count_first_booking_view.offer_id = offer.id
        );
    """


def define_enriched_offer_data_full_query(dataset, table_prefix=""):
    return f"""
        {define_is_physical_view_query(dataset=dataset, table_prefix=table_prefix)}
        {define_is_outing_view_query(dataset=dataset, table_prefix=table_prefix)}
        {define_offer_booking_information_view_query(dataset=dataset, table_prefix=table_prefix)}
        {define_count_favorites_view_query(dataset=dataset, table_prefix=table_prefix)}
        {define_sum_stock_view_query(dataset=dataset, table_prefix=table_prefix)}
        {define_humanized_id_query(table=f"{table_prefix}offer", dataset=dataset)}
        {define_count_first_booking_query(dataset=dataset, table_prefix=table_prefix)}
        {define_enriched_offer_data_query(dataset=dataset, table_prefix=table_prefix)}
    """
