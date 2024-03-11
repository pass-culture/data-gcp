{{ create_humanize_id_function() }} 

WITH offer_humanized_id AS (
    SELECT
        offer_id,
        humanize_id(offer_id) AS humanized_id,
    FROM
        `{{ bigquery_clean_dataset }}`.applicative_database_offer
    WHERE
        offer_id is not NULL
),
offer_booking_information_view AS (
    SELECT
        offer.offer_id,
        COUNT(DISTINCT booking.booking_id) AS count_booking,
        COUNT(
            DISTINCT CASE
                WHEN booking.booking_is_cancelled THEN booking.booking_id
                ELSE NULL
            END
        ) AS count_booking_cancelled,
        COUNT(
            DISTINCT CASE
                WHEN booking.booking_is_used THEN booking.booking_id
                ELSE NULL
            END
        ) AS count_booking_confirm
    FROM
        `{{ bigquery_clean_dataset }}`.applicative_database_offer AS offer
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_stock AS stock ON stock.offer_id = offer.offer_id
        LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_booking AS booking ON stock.stock_id = booking.stock_id
    GROUP BY
        offer_id
),
count_favorites_view AS (
    SELECT
        offerId,
        COUNT(*) AS count_favorite
    FROM
        `{{ bigquery_clean_dataset }}`.applicative_database_favorite AS favorite
    GROUP BY
        offerId
),
sum_stock_view AS (
    SELECT
        offer_id,
        SUM(stock_quantity) AS stock
    FROM
        `{{ bigquery_clean_dataset }}`.applicative_database_stock AS stock
    GROUP BY
        offer_id
),
count_first_booking_view AS (
    SELECT
        offer_id,
        count(*) as first_booking_cnt
    FROM
        (
            SELECT
                stock.offer_id,
                rank() OVER (
                    PARTITION BY booking.user_id
                    ORDER BY
                        booking.booking_creation_date,
                        booking.booking_id
                ) AS booking_rank
            FROM
                `{{ bigquery_clean_dataset }}`.applicative_database_booking AS booking
                LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_stock AS stock on stock.stock_id = booking.stock_id
        ) c
    WHERE
        c.booking_rank = 1
    GROUP BY
        offer_id
    ORDER BY
        first_booking_cnt DESC
),
last_stock AS (
    SELECT
        offer_id,
        stock_price AS last_stock_price
    FROM
        (
            SELECT
                offer.offer_id,
                stock.stock_price,
                rank() OVER (
                    PARTITION BY stock.offer_id
                    ORDER BY
                        stock.stock_creation_date DESC,
                        stock.stock_id DESC
                ) AS rang_stock
            FROM
                `{{ bigquery_clean_dataset }}`.applicative_database_offer AS offer
                JOIN `{{ bigquery_clean_dataset }}`.cleaned_stock AS stock on stock.offer_id = offer.offer_id
        ) c
    WHERE
        c.rang_stock = 1
),
mediation AS (
    SELECT 
        offer_id, 
        humanize_id(id) as mediation_humanized_id
    FROM (
        SELECT
            id,
            offerId as offer_id,
            ROW_NUMBER() OVER (PARTITION BY offerId ORDER BY dateModifiedAtLastProvider DESC) as rnk
        FROM `{{ bigquery_clean_dataset }}.applicative_database_mediation`
        WHERE isActive
        ) inn
    WHERE rnk = 1
)
SELECT
    offerer.offerer_id,
    offerer.offerer_name,
    venue.venue_id,
    CASE WHEN venue.venue_is_permanent THEN CONCAT("venue-",venue.venue_id)
         ELSE CONCAT("offerer-", offerer.offerer_id) END AS partner_id,
    venue.venue_name,
    venue.venue_department_code,
    offer.offer_id,
    offer.offer_product_id,
    humanize_id(offer.offer_product_id) as offer_product_humanized_id,
    offer.offer_id_at_providers,
    CASE WHEN offer.offer_id_at_providers IS NOT NULL THEN TRUE ELSE FALSE END as is_synchronised,
    offer.offer_name,
    offer_description,
    offer.offer_subcategoryId,
    subcategories.category_id offer_category_id,
    last_stock.last_stock_price,
    offer.offer_creation_date,
    offer.offer_is_duo,
    offer_ids.item_id,
    CASE
        WHEN (
            offer.offer_subcategoryId <> 'JEU_EN_LIGNE'
            AND offer.offer_subcategoryId <> 'JEU_SUPPORT_PHYSIQUE'
            AND offer.offer_subcategoryId <> 'ABO_JEU_VIDEO'
            AND offer.offer_subcategoryId <> 'ABO_LUDOTHEQUE'
            AND (
                offer.offer_url IS NULL -- not numerical
                OR last_stock.last_stock_price = 0
                OR subcategories.id = 'LIVRE_NUMERIQUE'
                OR subcategories.id = 'ABO_LIVRE_NUMERIQUE'
                OR subcategories.id = 'TELECHARGEMENT_LIVRE_AUDIO'
                OR subcategories.category_id = 'MEDIA'
            )
        ) THEN TRUE
        ELSE FALSE
    END AS offer_is_underage_selectable,
    CASE
        WHEN offer.offer_id IN (
            SELECT
                offer_id
            FROM
                `{{ bigquery_clean_dataset }}`.bookable_offer
        ) THEN TRUE
        ELSE FALSE
    END AS offer_is_bookable,
    venue.venue_is_virtual,
    subcategories.is_physical_deposit as physical_goods,
    subcategories.is_event as outing,
    COALESCE(
        offer_booking_information_view.count_booking,
        0.0
    ) AS booking_cnt,
    COALESCE(
        offer_booking_information_view.count_booking_cancelled,
        0.0
    ) AS booking_cancelled_cnt,
    COALESCE(
        offer_booking_information_view.count_booking_confirm,
        0.0
    ) AS booking_confirm_cnt,
    COALESCE(count_favorites_view.count_favorite, 0.0) AS favourite_cnt,
    sum_stock_view.stock AS stock,
    offer_humanized_id.humanized_id AS offer_humanized_id,
    CONCAT(
        'https://passculture.pro/offre/individuelle/',
        offer.offer_id,
        '/informations'
    ) AS passculture_pro_url,
    CONCAT('https://passculture.app/offre/', offer.offer_id) AS webapp_url,
    offer.offer_url as URL,
    offer.offer_is_national as is_national,
    offer.offer_is_active as is_active,
    offer.offer_validation as offer_validation,
    mediation.mediation_humanized_id,
    count_first_booking_view.first_booking_cnt,
    offer_extracted_data.author,
    offer_extracted_data.performer,
    offer_extracted_data.stageDirector,
    offer_extracted_data.theater_movie_id,
    offer_extracted_data.theater_room_id,
    offer_extracted_data.speaker,
    offer_extracted_data.movie_type,
    offer_extracted_data.visa,
    offer_extracted_data.releaseDate,
    offer_extracted_data.genres,
    offer_extracted_data.companies,
    offer_extracted_data.countries,
    offer_extracted_data.casting,
    offer_extracted_data.isbn,
    offer_extracted_data.titelive_gtl_id,
    isbn_rayon_editor.rayon,
    isbn_rayon_editor.book_editor,
    CASE
        WHEN subcategories.category_id <> 'MUSIQUE_LIVE'
        AND offer_extracted_data.showType IS NOT NULL THEN offer_extracted_data.showType
        WHEN subcategories.category_id = 'MUSIQUE_LIVE' THEN offer_extracted_data.musicType
        WHEN subcategories.category_id <> 'SPECTACLE'
        AND offer_extracted_data.musicType IS NOT NULL THEN offer_extracted_data.musicType
    END AS type,
    CASE
        WHEN subcategories.category_id <> 'MUSIQUE_LIVE'
        AND offer_extracted_data.showSubType IS NOT NULL THEN offer_extracted_data.showSubType
        WHEN subcategories.category_id = 'MUSIQUE_LIVE' THEN offer_extracted_data.musicSubtype
        WHEN subcategories.category_id <> 'SPECTACLE'
        AND offer_extracted_data.musicsubType IS NOT NULL THEN offer_extracted_data.musicSubtype
    END AS subType
FROM
    `{{ bigquery_clean_dataset }}`.applicative_database_offer AS offer
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.subcategories subcategories ON offer.offer_subcategoryId = subcategories.id
    LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_venue AS venue ON offer.venue_id = venue.venue_id
    LEFT JOIN `{{ bigquery_clean_dataset }}`.applicative_database_offerer AS offerer ON venue.venue_managing_offerer_id = offerer.offerer_id
    LEFT JOIN `{{ bigquery_clean_dataset }}`.offer_item_ids AS offer_ids on offer.offer_id=offer_ids.offer_id
    LEFT JOIN offer_booking_information_view ON offer_booking_information_view.offer_id = offer.offer_id
    LEFT JOIN count_favorites_view ON count_favorites_view.offerId = offer.offer_id
    LEFT JOIN sum_stock_view ON sum_stock_view.offer_id = offer.offer_id
    LEFT JOIN offer_humanized_id AS offer_humanized_id ON offer_humanized_id.offer_id = offer.offer_id
    LEFT JOIN count_first_booking_view ON count_first_booking_view.offer_id = offer.offer_id
    LEFT JOIN last_stock ON last_stock.offer_id = offer.offer_id
    LEFT JOIN `{{ bigquery_clean_dataset }}`.offer_extracted_data offer_extracted_data ON offer_extracted_data.offer_id = offer.offer_id
    LEFT JOIN mediation ON offer.offer_id = mediation.offer_id
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.isbn_rayon_editor AS isbn_rayon_editor ON offer_extracted_data.isbn = isbn_rayon_editor.isbn

WHERE
    offer.offer_validation = 'APPROVED'