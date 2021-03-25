def temporary_booking_table(gcp_project, bigquery_analytics_dataset):
    return f"""
        WITH bookings AS (
            SELECT user_id, offer.offer_type,  venue_is_virtual 
            FROM `{gcp_project}.{bigquery_analytics_dataset}.applicative_database_booking` booking
            LEFT JOIN `{gcp_project}.{bigquery_analytics_dataset}.applicative_database_stock` stock
            ON booking.stock_id = stock.stock_id
            LEFT JOIN `{gcp_project}.{bigquery_analytics_dataset}.applicative_database_offer` offer
            ON stock.offer_id = offer.offer_id
            LEFT JOIN `{gcp_project}.{bigquery_analytics_dataset}.applicative_database_venue` venue
            ON venue.venue_id = offer.venue_id
            WHERE booking.booking_is_cancelled IS false
        )
    """


def enrich_booked_categories(gcp_project, bigquery_analytics_dataset, version):
    booking_query = temporary_booking_table(gcp_project, bigquery_analytics_dataset)
    query = """
        SELECT user_id,
        SUM(CAST(offer_type = 'ThingType.AUDIOVISUEL' AS INT64)) > 0 AS audiovisuel,
        SUM(CAST(offer_type = 'ThingType.INSTRUMENT' AS INT64)) > 0 AS instrument,
        SUM(CAST(offer_type in ('ThingType.JEUX_VIDEO_ABO', 'ThingType.JEUX_VIDEO') AS INT64)) > 0 AS jeux_videos,
        SUM(CAST(offer_type = 'ThingType.LIVRE_EDITION' or offer_type = 'ThingType.LIVRE_AUDIO' AS INT64)) > 0 AS livre,
        SUM(CAST(offer_type in ('EventType.MUSEES_PATRIMOINE', 'ThingType.MUSEES_PATRIMOINE_ABO') AS INT64)) > 0 AS musees_patrimoine,
        SUM(CAST(offer_type in ('EventType.PRATIQUE_ARTISTIQUE', 'ThingType.PRATIQUE_ARTISTIQUE_ABO') AS INT64)) > 0 AS pratique_artistique,
        SUM(CAST(offer_type = 'ThingType.PRESSE_ABO' AS INT64)) > 0 AS presse,
        SUM(CAST(offer_type in ('EventType.SPECTACLE_VIVANT', 'ThingType.SPECTACLE_VIVANT_ABO') AS INT64)) > 0 AS spectacle_vivant,
    """
    if version == 1:
        query += """    SUM(CAST(offer_type in ('EventType.CONFERENCE_DEBAT_DEDICACE', 'ThingType.OEUVRE_ART', 'EventType.JEUX') AS INT64)) > 0 AS autre,
        SUM(CAST(offer_type in ('EventType.CINEMA', 'ThingType.CINEMA_ABO', 'ThingType.CINEMA_CARD') AS INT64)) > 0 AS cinema,
        SUM(CAST(offer_type in ('EventType.MUSIQUE', 'ThingType.MUSIQUE_ABO') AS INT64)) > 0 AS musique
        FROM bookings
        GROUP BY user_id
        """
    if version == 2:
        query += """    SUM(CAST(offer_type = 'EventType.CONFERENCE_DEBAT_DEDICACE' AS INT64)) > 0 AS autre,
        SUM(CAST(offer_type in ('EventType.CINEMA', 'ThingType.CINEMA_CARD') AS INT64)) > 0 AS cinema,
        SUM(CAST(offer_type in ('EventType.MUSIQUE', 'ThingType.MUSIQUE_ABO', 'ThingType.MUSIQUE') AS INT64)) > 0 AS musique
        FROM bookings
        GROUP BY user_id
        """
    return f"{booking_query}{query}"


def define_user_booked_audiovisuel_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE user_booked_audiovisuel AS (
          SELECT booking.user_id as user_id FROM `{dataset}.{table_prefix}booking` booking
          JOIN `{dataset}.{table_prefix}stock`stock ON stock.stock_id = booking.stock_id
          JOIN `{dataset}.{table_prefix}offer` offer ON offer.offer_id = stock.offer_id
          WHERE offer.offer_type ='ThingType.AUDIOVISUEL'
          GROUP BY booking.user_id
        );
    """


def define_user_booked_cinema_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE user_booked_cinema AS (
          SELECT booking.user_id as user_id FROM `{dataset}.{table_prefix}booking` booking
          JOIN `{dataset}.{table_prefix}stock`stock on stock.stock_id = booking.stock_id
          JOIN `{dataset}.{table_prefix}offer` offer on offer.offer_id = stock.offer_id
          WHERE offer.offer_type IN ('EventType.CINEMA', 'ThingType.CINEMA_ABO', 'ThingType.CINEMA_CARD')
          GROUP BY booking.user_id
        );
    """


def define_user_booked_instrument_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE user_booked_instrument AS (
          SELECT booking.user_id as user_id FROM `{dataset}.{table_prefix}booking` booking
          JOIN `{dataset}.{table_prefix}stock`stock on stock.stock_id = booking.stock_id
          JOIN `{dataset}.{table_prefix}offer` offer on offer.offer_id = stock.offer_id
          WHERE offer.offer_type ='ThingType.INSTRUMENT'
          GROUP BY booking.user_id
        );
    """


def define_user_booked_jeux_video_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE user_booked_jeux_video AS (
          SELECT booking.user_id as user_id FROM `{dataset}.{table_prefix}booking` booking
          JOIN `{dataset}.{table_prefix}stock`stock on stock.stock_id = booking.stock_id
          JOIN `{dataset}.{table_prefix}offer` offer on offer.offer_id = stock.offer_id
          WHERE offer.offer_type IN ('ThingType.JEUX_VIDEO', 'ThingType.JEUX_VIDEO_ABO')
          GROUP BY booking.user_id
        );
    """


def define_user_booked_livre_num_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE user_booked_livre_num AS (
          SELECT booking.user_id as user_id FROM `{dataset}.{table_prefix}booking` booking
          JOIN `{dataset}.{table_prefix}stock`stock on stock.stock_id = booking.stock_id
          JOIN `{dataset}.{table_prefix}offer` offer on offer.offer_id = stock.offer_id
          JOIN `{dataset}.{table_prefix}venue` venue on offer.venue_id = venue.venue_id
            AND venue.venue_is_virtual
          WHERE offer.offer_type = 'ThingType.LIVRE_EDITION'
          GROUP BY booking.user_id
        );
    """


def define_user_booked_livre_papier_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE user_booked_livre_papier AS (
          SELECT booking.user_id as user_id FROM `{dataset}.{table_prefix}booking` booking
          JOIN `{dataset}.{table_prefix}stock`stock on stock.stock_id = booking.stock_id
          JOIN `{dataset}.{table_prefix}offer` offer on offer.offer_id = stock.offer_id
          JOIN `{dataset}.{table_prefix}venue` venue on offer.venue_id = venue.venue_id
            AND NOT venue.venue_is_virtual
          WHERE offer.offer_type = 'ThingType.LIVRE_EDITION'
          GROUP BY booking.user_id
        );
    """


def define_user_booked_musee_patrimoine_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE user_booked_musee_patrimoine AS (
          SELECT booking.user_id as user_id FROM `{dataset}.{table_prefix}booking` booking
          JOIN `{dataset}.{table_prefix}stock`stock on stock.stock_id = booking.stock_id
          JOIN `{dataset}.{table_prefix}offer` offer on offer.offer_id = stock.offer_id
          WHERE offer.offer_type IN ('EventType.MUSEES_PATRIMOINE', 'ThingType.MUSEES_PATRIMOINE_ABO')
          GROUP BY booking.user_id
        );
    """


def define_user_booked_musique_live_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE user_booked_musique_live AS (
          SELECT booking.user_id as user_id FROM `{dataset}.{table_prefix}booking` booking
          JOIN `{dataset}.{table_prefix}stock`stock on stock.stock_id = booking.stock_id
          JOIN `{dataset}.{table_prefix}offer` offer on offer.offer_id = stock.offer_id
          WHERE offer.offer_type IN ('EventType.MUSIQUE', 'ThingType.MUSIQUE_ABO')
          GROUP BY booking.user_id
        );
    """


def define_user_booked_musique_cd_vynils(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE user_booked_musique_cd_vynils AS (
          SELECT booking.user_id as user_id FROM `{dataset}.{table_prefix}booking` booking
          JOIN `{dataset}.{table_prefix}stock`stock on stock.stock_id = booking.stock_id
          JOIN `{dataset}.{table_prefix}offer` offer on offer.offer_id = stock.offer_id
          JOIN `{dataset}.{table_prefix}venue` venue on offer.venue_id = venue.venue_id
            AND NOT venue.venue_is_virtual
          WHERE offer.offer_type = 'ThingType.MUSIQUE'
          GROUP BY booking.user_id
        );
    """


def define_user_booked_musique_numerique(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE user_booked_musique_numerique AS (
          SELECT booking.user_id as user_id FROM `{dataset}.{table_prefix}booking` booking
          JOIN `{dataset}.{table_prefix}stock`stock on stock.stock_id = booking.stock_id
          JOIN `{dataset}.{table_prefix}offer` offer on offer.offer_id = stock.offer_id
          JOIN `{dataset}.{table_prefix}venue` venue on offer.venue_id = venue.venue_id
            AND venue.venue_is_virtual
          WHERE offer.offer_type = 'ThingType.MUSIQUE'
          GROUP BY booking.user_id
        );
    """


def define_user_booked_pratique_artistique_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE user_booked_pratique_artistique AS (
          SELECT booking.user_id as user_id FROM `{dataset}.{table_prefix}booking` booking
          JOIN `{dataset}.{table_prefix}stock`stock on stock.stock_id = booking.stock_id
          JOIN `{dataset}.{table_prefix}offer` offer on offer.offer_id = stock.offer_id
          WHERE offer.offer_type IN ('EventType.PRATIQUE_ARTISTIQUE','ThingType.PRATIQUE_ARTISTIQUE')
          GROUP BY booking.user_id
        );
    """


def define_user_booked_spectacle_vivant_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE user_booked_spectacle_vivant AS (
          SELECT booking.user_id as user_id FROM `{dataset}.{table_prefix}booking` booking
          JOIN `{dataset}.{table_prefix}stock`stock on stock.stock_id = booking.stock_id
          JOIN `{dataset}.{table_prefix}offer` offer on offer.offer_id = stock.offer_id
          WHERE offer.offer_type IN ('EventType.SPECTACLE_VIVANT', 'ThingType.SPECTACLE_VIVANT_ABO')
          GROUP BY booking.user_id
        );
    """


def define_enriched_booked_categories_query(dataset, table_prefix=""):
    return f"""
      CREATE OR REPLACE TABLE {dataset}.enriched_booked_categories_data AS (
        SELECT
          distinct(booking.user_id),
          CASE WHEN user_booked_audiovisuel.user_id is NOT NULL THEN TRUE ELSE FALSE END as audiovisuel,
          CASE WHEN user_booked_cinema.user_id is NOT NULL THEN TRUE ELSE FALSE END as cinema,
          CASE WHEN user_booked_instrument.user_id is NOT NULL THEN TRUE ELSE FALSE END as instrument,
          CASE WHEN user_booked_jeux_video.user_id is NOT NULL THEN TRUE ELSE FALSE END as jeux_video,
          CASE WHEN user_booked_livre_num.user_id is NOT NULL THEN TRUE ELSE FALSE END as livre_numerique,
          CASE WHEN user_booked_livre_papier.user_id is NOT NULL THEN TRUE ELSE FALSE END as livre_papier,
          CASE WHEN user_booked_musee_patrimoine.user_id is NOT NULL THEN TRUE ELSE FALSE END as musee_patrimoine,
          CASE WHEN user_booked_musique_live.user_id is NOT NULL THEN TRUE ELSE FALSE END as musique_live,
          CASE WHEN user_booked_musique_cd_vynils.user_id is NOT NULL THEN TRUE ELSE FALSE END as musique_cd_vynils,
          CASE WHEN user_booked_musique_numerique.user_id is NOT NULL THEN TRUE ELSE FALSE END as musique_numerique,
          CASE WHEN user_booked_pratique_artistique.user_id is NOT NULL THEN TRUE ELSE FALSE END as pratique_artistique,
          CASE WHEN user_booked_spectacle_vivant.user_id is NOT NULL THEN TRUE ELSE FALSE END as spectacle_vivant,
        FROM `{dataset}.{table_prefix}booking`booking
        LEFT JOIN user_booked_audiovisuel on booking.user_id = user_booked_audiovisuel.user_id
        LEFT JOIN user_booked_cinema on booking.user_id = user_booked_cinema.user_id
        LEFT JOIN user_booked_instrument on booking.user_id = user_booked_instrument.user_id
        LEFT JOIN user_booked_jeux_video on booking.user_id = user_booked_jeux_video.user_id
        LEFT JOIN user_booked_livre_num on booking.user_id = user_booked_livre_num.user_id
        LEFT JOIN user_booked_livre_papier on booking.user_id = user_booked_livre_papier.user_id
        LEFT JOIN user_booked_musee_patrimoine on booking.user_id = user_booked_musee_patrimoine.user_id
        LEFT JOIN user_booked_musique_live on booking.user_id = user_booked_musique_live.user_id
        LEFT JOIN user_booked_musique_cd_vynils on booking.user_id = user_booked_musique_cd_vynils.user_id
        LEFT JOIN user_booked_musique_numerique on booking.user_id = user_booked_musique_numerique.user_id
        LEFT JOIN user_booked_pratique_artistique on booking.user_id = user_booked_pratique_artistique.user_id
        LEFT JOIN user_booked_spectacle_vivant on booking.user_id = user_booked_spectacle_vivant.user_id
        ORDER BY booking.user_id
      )
  """


def define_enriched_booked_categories_data_full_query(dataset, table_prefix=""):
    return f"""
    {define_user_booked_audiovisuel_query(dataset, table_prefix)}
    {define_user_booked_cinema_query(dataset, table_prefix)}
    {define_user_booked_instrument_query(dataset, table_prefix)}
    {define_user_booked_jeux_video_query(dataset, table_prefix)}
    {define_user_booked_livre_num_query(dataset, table_prefix)}
    {define_user_booked_livre_papier_query(dataset, table_prefix)}
    {define_user_booked_musee_patrimoine_query(dataset, table_prefix)}
    {define_user_booked_musique_live_query(dataset, table_prefix)}
    {define_user_booked_musique_cd_vynils(dataset, table_prefix)}
    {define_user_booked_musique_numerique(dataset, table_prefix)}
    {define_user_booked_pratique_artistique_query(dataset, table_prefix)}
    {define_user_booked_spectacle_vivant_query(dataset, table_prefix)}
    {define_enriched_booked_categories_query(dataset, table_prefix)}
  """
