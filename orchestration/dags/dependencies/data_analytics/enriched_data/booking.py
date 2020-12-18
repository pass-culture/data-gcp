from dependencies.data_analytics.enriched_data.enriched_data_utils import (
    define_humanized_id_query,
)


def create_booking_amount_view(dataset):
    return f"""
        CREATE OR REPLACE TABLE {dataset}.booking_amount_view AS (
            SELECT 
                booking.id AS booking_id, coalesce(booking.amount, 0) * coalesce(booking.quantity, 0) AS montant_de_la_reservation
            FROM {dataset}.booking);
        """


def create_booking_payment_status_view(dataset):
    return f"""
        CREATE OR REPLACE TABLE {dataset}.booking_payment_status_view AS (
            SELECT
                booking.id AS booking_id,'Remboursé' AS rembourse
            FROM {dataset}.booking
            INNER JOIN {dataset}.payment 
                ON payment.bookingId = booking.id 
                AND payment.author IS NOT NULL
            INNER JOIN {dataset}.payment_status 
                ON payment.id = payment_status.paymentId 
                AND payment_status.status = 'SENT');
        """


def create_booking_ranking_view(dataset):
    return f"""
        CREATE OR REPLACE TABLE {dataset}.booking_ranking_view AS (
            SELECT 
                booking.id AS booking_id, rank() OVER (PARTITION BY booking.userId ORDER BY booking.dateCreated) AS classement_de_la_reservation
            FROM {dataset}.booking);
        """


def create_booking_ranking_in_category_view(dataset):
    return f"""
        CREATE OR REPLACE TABLE {dataset}.booking_ranking_in_category_view AS (
            SELECT 
                booking.id AS booking_id, rank() OVER (PARTITION BY booking.userId, offer.type ORDER BY booking.dateCreated) 
                AS classement_de_la_reservation_dans_la_meme_categorie
            FROM {dataset}.booking
            INNER JOIN {dataset}.stock ON booking.stockId = stock.id
            INNER JOIN {dataset}.offer ON stock.offerId = offer.id
            ORDER BY booking.id);
        """


def create_materialized_booking_intermediary_view(dataset):
    return f"""
        CREATE OR REPLACE TABLE {dataset}.booking_intermediary_view AS (
               SELECT booking.id,
                      booking_amount_view.montant_de_la_reservation,
                      booking_payment_status_view.rembourse,
                      booking_ranking_view.classement_de_la_reservation,
                      booking_ranking_in_category_view.classement_de_la_reservation_dans_la_meme_categorie
                 FROM {dataset}.booking
            LEFT JOIN {dataset}.booking_amount_view ON booking_amount_view.booking_id = booking.id
            LEFT JOIN {dataset}.booking_payment_status_view ON booking_payment_status_view.booking_id = booking.id
            LEFT JOIN {dataset}.booking_ranking_view ON booking_ranking_view.booking_id = booking.id
            LEFT JOIN {dataset}.booking_ranking_in_category_view ON booking_ranking_in_category_view.booking_id = booking.id
        );
    """


def create_materialized_enriched_booking_view(dataset):
    return f"""
        CREATE OR REPLACE TABLE {dataset}.enriched_booking_data AS (
             SELECT
                booking.id,
                booking.dateCreated AS date_de_reservation,
                booking.quantity,
                booking.amount,
                booking.isCancelled,
                booking.isUsed,
                booking.cancellationDate AS date_annulation,
                stock.beginningDatetime AS date_de_debut_evenement,
                offer.type AS type_offre,
                offer.name AS nom_offre,
                coalesce(venue.publicName, venue.name) AS nom_du_lieu,
                venue_label.label AS label_du_lieu,
                venue_type.label AS type_de_lieu,
                venue.departementCode AS departement_du_lieu,
                offerer.name AS nom_de_la_structure,
                user.departementCode AS departement_utilisateur,
                user.dateCreated AS date_de_creation_utilisateur,
                booking_intermediary_view.montant_de_la_reservation,
                CASE WHEN booking_intermediary_view.rembourse = 'Remboursé'
                    THEN True ELSE False END AS rembourse,
                CASE WHEN
                    offer.type IN ('ThingType.INSTRUMENT','ThingType.JEUX','ThingType.LIVRE_EDITION','ThingType.MUSIQUE','ThingType.OEUVRE_ART','ThingType.AUDIOVISUEL')
                    AND venue.name <> 'Offre numérique'
                        THEN true else false end as reservation_de_bien_physique,
                CASE WHEN venue.name = 'Offre numérique'
                    THEN true else false end as reservation_de_bien_numerique,
                CASE WHEN
                    offer.type NOT IN ('ThingType.INSTRUMENT','ThingType.JEUX','ThingType.LIVRE_EDITION','ThingType.MUSIQUE','ThingType.OEUVRE_ART','ThingType.AUDIOVISUEL')
                    AND venue.name <> 'Offre numérique'
                        THEN true else false end as reservation_de_sortie,
                booking_intermediary_view.classement_de_la_reservation,
                booking_intermediary_view.classement_de_la_reservation_dans_la_meme_categorie
            FROM {dataset}.booking
            INNER JOIN {dataset}.stock
                ON booking.stockId = stock.id
            INNER JOIN {dataset}.offer
                ON offer.id = stock.offerId
                AND offer.type NOT IN ('ThingType.ACTIVATION','EventType.ACTIVATION')
            INNER JOIN {dataset}.venue
                ON venue.id = offer.venueId
            INNER JOIN {dataset}.offerer
                ON venue.managingOffererId = offerer.id
            INNER JOIN {dataset}.user
                ON user.id = booking.userId
            LEFT JOIN {dataset}.venue_type
                ON venue.venueTypeId = venue_type.id
            LEFT JOIN {dataset}.venue_label
                ON venue.venueLabelId = venue_label.id
            LEFT JOIN {dataset}.booking_intermediary_view ON booking_intermediary_view.id = booking.id
        );
        """


def define_enriched_booking_data_full_query(dataset):
    return f"""
         {create_booking_amount_view(dataset=dataset)}
         {create_booking_payment_status_view(dataset=dataset)}
         {create_booking_ranking_view(dataset=dataset)}
         {create_booking_ranking_in_category_view(dataset=dataset)}
         {create_materialized_booking_intermediary_view(dataset=dataset)}
         {create_materialized_enriched_booking_view(dataset=dataset)}
    """
