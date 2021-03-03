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
