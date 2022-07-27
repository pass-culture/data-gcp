from dependencies.import_analytics.enriched_data.enriched_data_utils import (
    create_humanize_id_function,
    create_temp_humanize_id,
)


def define_offer_booking_information_view_query(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE offer_booking_information_view AS
            SELECT
                offer.offer_id,
                COUNT(DISTINCT booking.booking_id) AS count_booking,
                COUNT(DISTINCT CASE WHEN booking.booking_is_cancelled THEN booking.booking_id ELSE NULL END)
                    AS count_booking_cancelled,
                COUNT(DISTINCT CASE WHEN booking.booking_is_used THEN booking.booking_id ELSE NULL END)
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


def define_last_stock_price(dataset, table_prefix=""):
    return f"""
        CREATE TEMP TABLE last_stock AS
            SELECT
                offer_id, stock_price AS last_stock_price
            FROM (
                SELECT
                    offer.offer_id,
                    stock.stock_price,
                    rank() OVER (PARTITION BY stock.offer_id ORDER BY stock.stock_creation_date DESC, stock.stock_id DESC)
                    AS rang_stock
                FROM {dataset}.{table_prefix}offer AS offer
                JOIN {dataset}.{table_prefix}stock AS stock on stock.offer_id = offer.offer_id) c
            WHERE c.rang_stock = 1;
    """


def define_enriched_offer_data_query(analytics_dataset, clean_dataset, table_prefix=""):
    return f"""
        CREATE OR REPLACE TABLE {analytics_dataset}.enriched_offer_data AS (
            SELECT
                offerer.offerer_id,
                offerer.offerer_name,
                venue.venue_id,
                venue.venue_name,
                venue.venue_department_code,
                offer.offer_id,
                offer.offer_name,
                offer.offer_subcategoryId,
                last_stock.last_stock_price,
                offer.offer_creation_date,
                offer.offer_is_duo,
                offer.offer_is_educational,
                CASE WHEN (offer.offer_subcategoryId <> 'JEU_EN_LIGNE'
                    AND offer.offer_subcategoryId <> 'JEU_SUPPORT_PHYSIQUE'
                    AND offer.offer_subcategoryId <> 'ABO_JEU_VIDEO'
                    AND offer.offer_subcategoryId <> 'ABO_LUDOTHEQUE'
                    AND NOT offer.offer_is_educational
                    AND (offer.offer_url IS NULL
                    OR last_stock.last_stock_price = 0
                    OR subcategories.id = 'LIVRE_NUMERIQUE'
                    OR subcategories.id = 'ABO_LIVRE_NUMERIQUE'
                    OR subcategories.id = 'TELECHARGEMENT_LIVRE_AUDIO'
                    OR subcategories.category_id = 'MEDIA')) THEN TRUE ELSE FALSE END AS offer_is_underage_selectable,
                    CASE WHEN offer.offer_id IN (
                        SELECT enriched_stock.offer_id
                        FROM {analytics_dataset}.enriched_stock_data AS enriched_stock
                        JOIN {analytics_dataset}.{table_prefix}stock AS stock ON stock.stock_id = enriched_stock.stock_id AND NOT stock_is_soft_deleted
                        JOIN {analytics_dataset}.{table_prefix}offer AS offer
                        ON enriched_stock.offer_id = offer.offer_id AND offer.offer_is_active
                        WHERE (
                        (DATE(enriched_stock.stock_booking_limit_date) > CURRENT_DATE OR enriched_stock.stock_booking_limit_date IS NULL)
                        AND (DATE(enriched_stock.stock_beginning_date) > CURRENT_DATE OR enriched_stock.stock_beginning_date IS NULL)
                        AND offer.offer_is_active
                        AND (available_stock_information > 0 OR available_stock_information IS NULL))
                             ) THEN TRUE ELSE FALSE END AS offer_is_bookable,
                venue.venue_is_virtual,
                subcategories.is_physical_deposit as physical_goods,
                subcategories.is_event as outing,
                COALESCE(offer_booking_information_view.count_booking, 0.0) AS booking_cnt,
                COALESCE(offer_booking_information_view.count_booking_cancelled, 0.0)
                    AS booking_cancelled_cnt,
                COALESCE(offer_booking_information_view.count_booking_confirm, 0.0)
                    AS booking_confirm_cnt,
                COALESCE(count_favorites_view.count_favorite, 0.0)
                    AS favourite_cnt,
                sum_stock_view.stock AS stock,
                offer_humanized_id.humanized_id AS offer_humanized_id,
                CONCAT('https://passculture.pro/offres/', offer_humanized_id.humanized_id, '/edition')
                    AS passculture_pro_url,
                CONCAT('https://passculture.app/offre/',offer.offer_id)
                    AS webapp_url,
                count_first_booking_view.first_booking_cnt,
                offer_tags.tag as offer_tag,
                offer_extracted_data.author,
                offer_extracted_data.performer, 
                offer_extracted_data.stageDirector,
                offer_extracted_data.theater_movie_id,
                offer_extracted_data.theater_room_id,
                offer_extracted_data.speaker, 
                offer_extracted_data.rayon,
                offer_extracted_data.movie_type,
                offer_extracted_data.visa,
                offer_extracted_data.releaseDate,
                offer_extracted_data.genres,
                offer_extracted_data.companies,
                offer_extracted_data.countries,
                offer_extracted_data.casting,
                offer_extracted_data.isbn,
                CASE 
                    WHEN subcategories.category_id <> 'MUSIQUE_LIVE' AND offer_extracted_data.showType IS NOT NULL THEN offer_extracted_data.showType 
                    WHEN subcategories.category_id = 'MUSIQUE_LIVE' THEN offer_extracted_data.musicType
                    WHEN subcategories.category_id <> 'SPECTACLE' AND offer_extracted_data.musicType IS NOT NULL THEN offer_extracted_data.musicType
                END AS type,
				CASE
                    WHEN subcategories.category_id <>'MUSIQUE_LIVE' AND offer_extracted_data.showSubType IS NOT NULL THEN offer_extracted_data.showSubType 
                    WHEN subcategories.category_id = 'MUSIQUE_LIVE' THEN offer_extracted_data.musicSubtype
                    WHEN subcategories.category_id <> 'SPECTACLE' AND offer_extracted_data.musicsubType IS NOT NULL THEN offer_extracted_data.musicSubtype
				END AS subType
            FROM {analytics_dataset}.{table_prefix}offer AS offer
            LEFT JOIN {analytics_dataset}.subcategories subcategories ON offer.offer_subcategoryId = subcategories.id
            LEFT JOIN {analytics_dataset}.{table_prefix}venue AS venue ON offer.venue_id = venue.venue_id
            LEFT JOIN {analytics_dataset}.{table_prefix}offerer AS offerer ON venue.venue_managing_offerer_id = offerer.offerer_id
            LEFT JOIN offer_booking_information_view ON offer_booking_information_view.offer_id = offer.offer_id
            LEFT JOIN count_favorites_view ON count_favorites_view.offerId = offer.offer_id
            LEFT JOIN sum_stock_view ON sum_stock_view.offer_id = offer.offer_id
            LEFT JOIN offer_humanized_id AS offer_humanized_id ON offer_humanized_id.offer_id = offer.offer_id
            LEFT JOIN count_first_booking_view ON count_first_booking_view.offer_id = offer.offer_id
            LEFT JOIN last_stock ON last_stock.offer_id = offer.offer_id
            LEFT JOIN {clean_dataset}.offer_extracted_data AS offer_extracted_data ON offer_extracted_data.offer_id = offer.offer_id
            LEFT JOIN {clean_dataset}.offer_tags AS offer_tags ON offer_tags.offer_id = offer.offer_id
            WHERE offer.offer_validation = 'APPROVED'

        );
    """


def define_enriched_offer_data_full_query(
    analytics_dataset, clean_dataset, table_prefix=""
):
    return f"""
        {define_offer_booking_information_view_query(dataset=analytics_dataset, table_prefix=table_prefix)}
        {define_count_favorites_view_query(dataset=analytics_dataset, table_prefix=table_prefix)}
        {define_sum_stock_view_query(dataset=analytics_dataset, table_prefix=table_prefix)}
        {create_humanize_id_function()}
        {create_temp_humanize_id(table="offer", dataset=analytics_dataset, table_prefix=table_prefix)}
        {define_count_first_booking_query(dataset=analytics_dataset, table_prefix=table_prefix)}
        {define_last_stock_price(dataset=analytics_dataset, table_prefix=table_prefix)}
        {define_enriched_offer_data_query(analytics_dataset =analytics_dataset, clean_dataset= clean_dataset,  table_prefix=table_prefix)}
    """
