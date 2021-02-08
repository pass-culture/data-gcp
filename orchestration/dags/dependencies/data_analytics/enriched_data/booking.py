from dependencies.data_analytics.enriched_data.enriched_data_utils import (
    define_humanized_id_query,
)


def create_booking_amount_view(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE booking_amount_view AS (
            SELECT
                booking.booking_id,
                coalesce(booking.booking_amount, 0) * coalesce(booking.booking_quantity, 0)
                AS booking_intermediary_amount
            FROM {dataset}.{table_prefix}booking AS booking);
        """


def create_booking_payment_status_view(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE booking_payment_status_view AS (
            SELECT
                booking.booking_id,'Remboursé' AS booking_reimburse
            FROM {dataset}.{table_prefix}booking AS booking
            INNER JOIN {dataset}.{table_prefix}payment AS payment
                ON payment.bookingId = booking.booking_id
                AND payment.author IS NOT NULL
            INNER JOIN {dataset}.{table_prefix}payment_status AS payment_status
                ON payment.id = payment_status.paymentId
                AND payment_status.status = 'SENT');
        """


def create_booking_ranking_view(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE booking_ranking_view AS (
            SELECT
                booking.booking_id,
                rank() OVER (PARTITION BY booking.user_id ORDER BY booking.booking_creation_date) AS booking_rank
            FROM {dataset}.{table_prefix}booking AS booking);
        """


def create_booking_ranking_in_category_view(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE booking_ranking_in_category_view AS (
            SELECT
                booking.booking_id,
                rank() OVER (PARTITION BY booking.user_id, offer.offer_type ORDER BY booking.booking_creation_date)
                AS same_category_booking_rank
            FROM {dataset}.{table_prefix}booking AS booking
            INNER JOIN {dataset}.{table_prefix}stock AS stock ON booking.stock_id = stock.stock_id
            INNER JOIN {dataset}.{table_prefix}offer AS offer ON stock.offer_id = offer.offer_id
            ORDER BY booking.booking_id);
        """


def create_materialized_booking_intermediary_view(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE booking_intermediary_view AS (
               SELECT booking.booking_id,
                      booking_amount_view.booking_intermediary_amount,
                      booking_payment_status_view.booking_reimburse,
                      booking_ranking_view.booking_rank,
                      booking_ranking_in_category_view.same_category_booking_rank
               FROM {dataset}.{table_prefix}booking AS booking
            LEFT JOIN booking_amount_view ON booking_amount_view.booking_id = booking.booking_id
            LEFT JOIN booking_payment_status_view ON booking_payment_status_view.booking_id = booking.booking_id
            LEFT JOIN booking_ranking_view ON booking_ranking_view.booking_id = booking.booking_id
            LEFT JOIN booking_ranking_in_category_view ON booking_ranking_in_category_view.booking_id = booking.booking_id
        );
    """


def create_materialized_enriched_booking_view(dataset, table_prefix=""):
    return f"""
        CREATE OR REPLACE TABLE {dataset}.enriched_booking_data AS (
             SELECT
                booking.booking_id,
                booking.booking_creation_date,
                booking.booking_quantity,
                booking.booking_amount,
                booking.booking_is_cancelled,
                booking.booking_is_used,
                booking.booking_cancellation_date,
                stock.stock_beginning_date,
                offer.offer_type,
                offer.offer_name,
                coalesce(venue.venue_public_name, venue.venue_name) AS venue_name,
                venue_label.label as venue_label,
                venue_type.label as venue_type_label,
                venue.venue_department_code,
                offerer.offerer_name,
                user.user_department_code,
                user.user_creation_date,
                booking_intermediary_view.booking_intermediary_amount,
                CASE WHEN booking_intermediary_view.booking_reimburse = 'Remboursé'
                    THEN True ELSE False END AS reimbursed,
                CASE WHEN
                    offer.offer_type IN ('ThingType.INSTRUMENT','ThingType.JEUX','ThingType.LIVRE_EDITION','ThingType.MUSIQUE','ThingType.OEUVRE_ART','ThingType.AUDIOVISUEL')
                    AND venue.venue_name <> 'Offre numérique'
                        THEN true else false end as physical_goods,
                CASE WHEN venue.venue_name = 'Offre numérique'
                    THEN true else false end as digital_goods,
                CASE WHEN
                    offer.offer_type NOT IN ('ThingType.INSTRUMENT','ThingType.JEUX','ThingType.LIVRE_EDITION','ThingType.MUSIQUE','ThingType.OEUVRE_ART','ThingType.AUDIOVISUEL')
                    AND venue.venue_name <> 'Offre numérique'
                        THEN true else false end as event,
                booking_intermediary_view.booking_rank,
                booking_intermediary_view.same_category_booking_rank
            FROM {dataset}.{table_prefix}booking AS booking
            INNER JOIN {dataset}.{table_prefix}stock AS stock
                ON booking.stock_id = stock.stock_id
            INNER JOIN {dataset}.{table_prefix}offer AS offer
                ON offer.offer_id = stock.offer_id
                AND offer.offer_type NOT IN ('ThingType.ACTIVATION','EventType.ACTIVATION')
            INNER JOIN {dataset}.{table_prefix}venue AS venue
                ON venue.venue_id = offer.venue_id
            INNER JOIN {dataset}.{table_prefix}offerer AS offerer
                ON venue.venue_managing_offerer_id = offerer.offerer_id
            INNER JOIN {dataset}.{table_prefix}user AS user
                ON user.user_id = booking.user_id
            LEFT JOIN {dataset}.{table_prefix}venue_type AS venue_type
                ON venue.venue_type_id = venue_type.id
            LEFT JOIN {dataset}.{table_prefix}venue_label AS venue_label
                ON venue.venue_label_id = venue_label.id
            LEFT JOIN booking_humanized_id AS booking_humanized_id ON booking_humanized_id.booking_id = booking.booking_id
            LEFT JOIN booking_intermediary_view ON booking_intermediary_view.booking_id = booking.booking_id
        );
        """


def define_enriched_booking_data_full_query(dataset, table_prefix=""):
    return f"""
         {create_booking_amount_view(dataset=dataset, table_prefix=table_prefix)}
         {create_booking_payment_status_view(dataset=dataset, table_prefix=table_prefix)}
         {create_booking_ranking_view(dataset=dataset, table_prefix=table_prefix)}
         {create_booking_ranking_in_category_view(dataset=dataset, table_prefix=table_prefix)}
         {create_materialized_booking_intermediary_view(dataset=dataset, table_prefix=table_prefix)}
         {define_humanized_id_query(table=f"booking", dataset=dataset, table_prefix=table_prefix)}
         {create_materialized_enriched_booking_view(dataset=dataset, table_prefix=table_prefix)}
    """
