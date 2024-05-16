{% set target_name = target.name %}
{% set target_schema = generate_schema_name("analytics_" ~ target_name) %}

{{ config(
    pre_hook="{{create_humanize_id_function()}}"
) }}

WITH stocks_grouped_by_offers AS (
        SELECT offer_id,
            SUM(total_available_stock) AS total_available_stock ,
            MAX(is_bookable) AS is_bookable,
            SUM(total_bookings) AS total_bookings,
            SUM(total_individual_bookings) AS total_individual_bookings,
            SUM(total_cancelled_individual_bookings) AS total_cancelled_individual_bookings,
            SUM(total_non_cancelled_individual_bookings) AS total_non_cancelled_individual_bookings,
            SUM(total_used_individual_bookings) AS total_used_individual_bookings,
            SUM(total_individual_theoretic_revenue) AS total_individual_theoretic_revenue,
            SUM(total_individual_real_revenue) AS total_individual_real_revenue,
            MIN(first_individual_booking_date) AS first_individual_booking_date,
            MAX(last_individual_booking_date) AS last_individual_booking_date,
            SUM(stock_quantity) AS total_stock_quantity,
            SUM(total_first_bookings) AS total_first_bookings,
            MAX(CASE WHEN stock_rk = 1 THEN stock_price ELSE NULL END) AS last_stock_price
        FROM {{ ref("int_applicative__stock") }}
        GROUP BY offer_id
),

total_favorites AS (
    SELECT
        offerId,
        COUNT(*) AS total_favorites
    FROM {{ source("raw", "applicative_database_favorite") }}
    GROUP BY offerId
)

SELECT
    o.offer_id,
    o.offer_id_at_providers,
    (o.offer_id_at_providers IS NOT NULL) AS is_synchronised,
    o.offer_modified_at_last_provider_date,
    DATE(o.offer_creation_date) AS offer_creation_date,
    o.offer_creation_date AS offer_created_at,
    o.offer_date_updated,
    o.offer_product_id,
    {{target_schema}}.humanize_id(o.offer_product_id) as offer_product_humanized_id,
    o.venue_id,
    o.offer_last_provider_id,
    o.booking_email,
    o.offer_is_active AS is_active,
    o.offer_name,
    o.offer_description,
    o.offer_url,
    CONCAT("https://passculture.pro/offre/individuelle/",o.offer_id,"/informations") AS passculture_pro_url,
    CONCAT("https://passculture.app/offre/", o.offer_id) AS webapp_url,
    o.offer_duration_minutes,
    o.offer_is_national AS is_national,
    o.offer_extra_data,
    o.offer_is_duo,
    o.offer_fields_updated,
    o.offer_withdrawal_details,
    o.offer_audio_disability_compliant,
    o.offer_mental_disability_compliant,
    o.offer_motor_disability_compliant,
    o.offer_visual_disability_compliant,
    o.offer_external_ticket_office_url,
    o.offer_validation,
    o.offer_last_validation_type,
    o.offer_subcategoryId AS offer_subcategory_id,
    o.offer_withdrawal_delay,
    o.booking_contact,
    o.author,
    o.performer,
    o.stageDirector AS stage_director,
    o.theater_movie_id,
    o.theater_room_id,
    o.speaker,
    o.movie_type,
    o.visa,
    o.releaseDate AS release_date,
    o.genres,
    o.companies,
    o.countries,
    o.casting,
    o.isbn,
    o.titelive_gtl_id,
    CASE WHEN (so.is_bookable = 1
        AND o.offer_is_active
        AND o.offer_validation = "APPROVED") THEN 1 ELSE 0 END AS is_bookable,
    so.total_available_stock,
    so.total_bookings,
    so.total_individual_bookings,
    so.total_cancelled_individual_bookings,
    so.total_non_cancelled_individual_bookings,
    so.total_used_individual_bookings,
    so.total_individual_theoretic_revenue,
    so.total_individual_real_revenue,
    so.first_individual_booking_date,
    so.last_individual_booking_date,
    so.total_stock_quantity,
    so.total_first_bookings,
    so.last_stock_price,
    tf.total_favorites,
    subcategories.is_physical_deposit as physical_goods,
    subcategories.is_digital_deposit digital_goods,
    subcategories.is_event as event,
    subcategories.category_id AS offer_category_id,
    isbn_rayon_editor.rayon,
    isbn_rayon_editor.book_editor,
    ii.item_id,
    m.mediation_humanized_id,
    {{target_schema}}.humanize_id(o.offer_id) AS offer_humanized_id,
    (o.offer_subcategoryId NOT IN ("JEU_EN_LIGNE", "JEU_SUPPORT_PHYSIQUE", "ABO_JEU_VIDEO", "ABO_LUDOTHEQUE")
            AND (
                o.offer_url IS NULL -- not numerical
                OR so.last_stock_price = 0
                OR subcategories.id = "LIVRE_NUMERIQUE"
                OR subcategories.id = "ABO_LIVRE_NUMERIQUE"
                OR subcategories.id = "TELECHARGEMENT_LIVRE_AUDIO"
                OR subcategories.category_id = "MEDIA"
            )
        ) AS offer_is_underage_selectable,
    CASE
            WHEN subcategories.category_id IN ("MUSIQUE_LIVE","MUSIQUE_ENREGISTREE") THEN "MUSIC"
            WHEN subcategories.category_id = "SPECTACLE" THEN "SHOW"
            WHEN subcategories.category_id = "CINEMA" THEN "MOVIE"
            WHEN subcategories.category_id = "LIVRE" THEN "BOOK"
        END AS offer_type_domain,
    CASE
        WHEN subcategories.category_id <> "MUSIQUE_LIVE" AND o.showType IS NOT NULL THEN o.showType
        WHEN subcategories.category_id = "MUSIQUE_LIVE" THEN o.musicType
        WHEN subcategories.category_id <> "SPECTACLE" AND o.musicType IS NOT NULL THEN o.musicType
    END AS type,
    CASE
        WHEN subcategories.category_id <> "MUSIQUE_LIVE" AND o.showSubType IS NOT NULL THEN o.showSubType
        WHEN subcategories.category_id = "MUSIQUE_LIVE" THEN o.musicSubtype
        WHEN subcategories.category_id <> "SPECTACLE" AND o.musicsubType IS NOT NULL THEN o.musicSubtype
    END AS subType,
FROM {{ ref("int_applicative__extract_offer") }} AS o
LEFT JOIN {{ ref("offer_item_ids") }} AS ii on ii.offer_id = o.offer_id
LEFT JOIN stocks_grouped_by_offers AS so ON so.offer_id = o.offer_id
LEFT JOIN total_favorites AS tf ON tf.offerId = o.offer_id
LEFT JOIN {{ source("clean","subcategories") }} AS subcategories ON o.offer_subcategoryId = subcategories.id
-- voir pour supprimer les dep à int_applicative__extract_offer et supprimer offer_extracted_data
LEFT JOIN {{ ref("isbn_rayon_editor") }} AS isbn_rayon_editor ON o.isbn = isbn_rayon_editor.isbn
LEFT JOIN {{ ref("int_applicative__mediation") }} AS m ON o.offer_id = m.offer_id
    AND m.is_active
    AND m.mediation_rown = 1
WHERE o.offer_subcategoryid NOT IN ("ACTIVATION_THING", "ACTIVATION_EVENT")
    AND (
        booking_email != "jeux-concours@passculture.app"
        OR booking_email IS NULL
        )
