{% set target_name = target.name %}
{% set target_schema = generate_schema_name('analytics_' ~ target_name) %}

{{ config(
    pre_hook="{{create_humanize_id_function()}}"
) }}

WITH offer_humanized_id AS (
    SELECT
        offer_id,
        offer_humanized_id
    FROM {{ ref('int_applicative__offer') }}
    WHERE
        offer_id is not NULL
),

venue_humanized_id AS (
    SELECT
        venue_id,
        venue_humanized_id
    FROM {{ ref('int_applicative__venue') }}
    WHERE
        venue_id is not NULL
),

offerer_humanized_id AS (
    SELECT
        offerer_id,
        {{ target_schema }}.humanize_id(offerer_id) as offerer_humanized_id
    FROM
        {{ ref('offerer') }}
    WHERE
        offerer_id is not NULL
),

bookings_days AS (
    SELECT DISTINCT
        offer.offer_id,
        DATE(booking_creation_date) AS booking_creation_date,
        COUNT(DISTINCT booking_id) OVER (PARTITION BY offer.offer_id, DATE(booking_creation_date)) AS cnt_bookings_day,
        IF(booking_is_cancelled, COUNT(DISTINCT booking_id) OVER (PARTITION BY offer.offer_id, DATE(booking_creation_date)), NULL) AS cnt_bookings_cancelled,
        IF(NOT booking_is_cancelled, COUNT(DISTINCT booking_id) OVER (PARTITION BY offer.offer_id, DATE(booking_creation_date)), NULL) AS cnt_bookings_confirm
    FROM {{ ref('booking') }} booking
        JOIN {{ ref('stock') }} stock USING(stock_id)
        JOIN {{ ref('offer') }} offer ON offer.offer_id = stock.offer_id
),

count_bookings AS (
    SELECT
        offer_id,
        MAX(cnt_bookings_day) AS max_bookings_in_day,
        MIN(booking_creation_date) AS first_booking_date,
        SUM(cnt_bookings_cancelled) AS cnt_bookings_cancelled,
        SUM(cnt_bookings_confirm) AS cnt_bookings_confirm
    FROM
        bookings_days
    GROUP BY
        offer_id
),

offer_stock_ids AS (
    SELECT
        offer_id,
        STRING_AGG(DISTINCT stock.stock_id, " ; " ORDER BY stock.stock_id) AS stocks_ids,
        DATE(MIN(stock_beginning_date)) AS first_stock_beginning_date,
        DATE(MAX(stock_booking_limit_date)) AS last_booking_limit_date,
        SUM(stock_quantity) AS offer_stock_quantity,
        SUM(available_stock_information.available_stock_information) AS available_stock_quantity
    FROM
        {{ ref('stock') }} stock
        JOIN {{ ref('available_stock_information') }} USING(stock_id)
    GROUP BY
        offer_id
),

last_stock AS (
    SELECT
        offer.offer_id,
        stock.stock_price AS last_stock_price
    FROM
         {{ ref('offer') }} AS offer
        JOIN  {{ ref('cleaned_stock') }} AS stock on stock.offer_id = offer.offer_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY stock.offer_id ORDER BY stock.stock_creation_date DESC, stock.stock_id DESC) = 1
),

offer_tags AS (
    SELECT
        offerId AS offer_id,
        STRING_AGG(name, " ; " ORDER BY CAST(criterion.id AS INT) DESC) AS playlist_tags
    FROM
        {{ ref('offer_criterion') }} offer_criterion
        JOIN {{ ref('criterion') }} criterion ON criterion.id = offer_criterion.criterionId
    GROUP BY
        offerId
),

offer_status AS (
    SELECT DISTINCT
        offer.offer_id,
        CASE
            WHEN offer.offer_is_active = FALSE THEN "INACTIVE"
            WHEN (offer.offer_validation LIKE "%APPROVED%" AND (SUM(available_stock_information.available_stock_information) OVER (PARTITION BY offer.offer_id)) <= 0) THEN "SOLD_OUT"
            WHEN (offer.offer_validation LIKE "%APPROVED%" AND (MAX(EXTRACT(DATE FROM stock.stock_booking_limit_date)) OVER (PARTITION BY offer.offer_id) < CURRENT_DATE())) THEN "EXPIRED"
            ELSE offer.offer_validation
        END AS offer_status
    FROM
        {{ ref('offer') }} offer
        LEFT JOIN {{ ref('stock') }} stock ON offer.offer_id = stock.offer_id
        JOIN {{ ref('available_stock_information') }} USING(stock_id)
),

offerer_tags AS (
    SELECT
        offerer_id,
        STRING_AGG(offerer_tag_label, " ; " ORDER BY CAST(offerer_id AS INT)) AS structure_tags
    FROM
        {{ ref('offerer_tag_mapping') }} offerer_tag_mapping
        LEFT JOIN {{ ref('offerer_tag') }}  offerer_tag ON offerer_tag_mapping.tag_id = offerer_tag.offerer_tag_id
    GROUP BY
        offerer_id
)

SELECT DISTINCT
    offer.offer_id,
    offer.offer_name,
    offer.offer_subcategoryid,
    subcategories.category_id,
    subcategories.is_physical_deposit as physical_goods,
    IF(subcategories.category_id = 'LIVRE', TRUE, FALSE) AS is_book,
    DATE(offer.offer_creation_date) AS offer_creation_date,
    offer.offer_external_ticket_office_url,
    IF(offer.offer_id_at_providers IS NULL, "manuel", "synchro") AS input_type,
    CASE
        WHEN offer.offer_id IN (
            SELECT
                stock.offer_id
            FROM
                {{ ref('stock') }} AS stock
                JOIN {{ ref('offer') }} AS offer ON stock.offer_id = offer.offer_id
                  AND offer.offer_is_active
                JOIN {{ ref('available_stock_information') }} ON available_stock_information.stock_id = stock.stock_id
            WHERE NOT stock_is_soft_deleted
            AND
                (
                    (
                        DATE(stock.stock_booking_limit_date) > CURRENT_DATE
                        OR stock.stock_booking_limit_date IS NULL
                    )
                    AND (
                        DATE(stock.stock_beginning_date) > CURRENT_DATE
                        OR stock.stock_beginning_date IS NULL
                    )
                    AND offer.offer_is_active
                    AND (
                        available_stock_information.available_stock_information > 0
                        OR available_stock_information.available_stock_information IS NULL
                    )
                )
        ) THEN TRUE
        ELSE FALSE
    END AS offer_is_bookable,
    offer.offer_is_duo,
    offer.offer_is_active,
    offer_status.offer_status,
    IF(offer_status.offer_status = 'SOLD_OUT', TRUE, FALSE) AS is_sold_out,
    venue.venue_managing_offerer_id AS offerer_id,
    offerer.offerer_name,
    venue.venue_id,
    venue.venue_name,
    venue.venue_public_name,
    region_dept.region_name,
    venue.venue_department_code,
    venue.venue_postal_code,
    venue.venue_type_code AS venue_type_label,
    IF(venue_label.venue_label IN("SMAC - Scène de musiques actuelles", "Théâtre lyrique conventionné d'intérêt national", "CNCM - Centre national de création musicale", "FRAC - Fonds régional d'art contemporain", "Scènes conventionnées", "Scène nationale", "Théâtres nationaux", "CAC - Centre d'art contemporain d'intérêt national", "CDCN - Centre de développement chorégraphique national", "Orchestre national en région", "CCN - Centre chorégraphique national", "CDN - Centre dramatique national", "Opéra national en région", "PNC - Pôle national du cirque","CNAREP - Centre national des arts de la rue et de l'espace public"), TRUE, FALSE) AS is_dgca,
    venue_label.venue_label AS venue_label,
    venue_humanized_id.venue_humanized_id,
    venue.venue_booking_email,
    venue_contact.venue_contact_phone_number,
    IF(siren_data.activitePrincipaleUniteLegale = "84.11Z", TRUE, FALSE) AS is_collectivity,
    offer_humanized_id.offer_humanized_id AS offer_humanized_id,
    CONCAT('https://passculture.pro/offre/individuelle/', offer_humanized_id.offer_humanized_id,'/informations') AS passculture_pro_url,
    CONCAT('https://passculture.app/offre/', offer.offer_id) AS webapp_url,
    CONCAT("https://passculture.pro/offres?structure=" , offerer_humanized_id.offerer_humanized_id) AS link_pc_pro,
    count_bookings.first_booking_date AS first_booking_date,
    COALESCE(count_bookings.max_bookings_in_day, 0) AS max_bookings_in_day,
    COALESCE(count_bookings.cnt_bookings_cancelled, 0) AS cnt_bookings_cancelled,
    COALESCE(count_bookings.cnt_bookings_confirm, 0) AS cnt_bookings_confirm,
    DATE_DIFF(count_bookings.first_booking_date, offer.offer_creation_date, DAY) AS diffdays_creation_firstbooking,
    offer_stock_ids.stocks_ids,
    offer_stock_ids.first_stock_beginning_date,
    offer_stock_ids.last_booking_limit_date,
    offer_stock_ids.offer_stock_quantity,
    offer_stock_ids.available_stock_quantity,
    SAFE_DIVIDE((offer_stock_ids.offer_stock_quantity - offer_stock_ids.available_stock_quantity), offer_stock_ids.offer_stock_quantity) AS fill_rate,
    last_stock.last_stock_price,
    offer_tags.playlist_tags,
    offerer_tags.structure_tags

FROM
    {{ ref('offer') }} offer
    LEFT JOIN {{ ref('venue') }}  venue ON venue.venue_id = offer.venue_id
    LEFT JOIN venue_humanized_id ON venue_humanized_id.venue_id = venue.venue_id
    LEFT JOIN {{ source('analytics', 'region_department') }}  region_dept ON region_dept.num_dep = venue.venue_department_code
    LEFT JOIN {{ ref('venue_label') }} venue_label ON venue_label.venue_label_id = venue.venue_label_id
    LEFT JOIN {{ ref('offerer') }} offerer ON offerer.offerer_id = venue.venue_managing_offerer_id
    LEFT JOIN offerer_humanized_id ON offerer_humanized_id.offerer_id = offerer.offerer_id
    LEFT JOIN {{ ref('siren_data') }} siren_data ON  siren_data.siren = offerer.offerer_siren
    LEFT JOIN offerer_tags ON offerer_tags.offerer_id = offerer.offerer_id
    LEFT JOIN {{ ref('venue_contact') }} venue_contact ON  venue_contact.venue_id = venue.venue_id
    LEFT JOIN offer_humanized_id AS offer_humanized_id ON offer_humanized_id.offer_id = offer.offer_id
    LEFT JOIN {{ source('clean','subcategories') }}  subcategories ON subcategories.id = offer.offer_subcategoryid
    LEFT JOIN count_bookings ON count_bookings.offer_id = offer.offer_id
    LEFT JOIN offer_stock_ids ON offer_stock_ids.offer_id = offer.offer_id
    LEFT JOIN last_stock ON last_stock.offer_id = offer.offer_id
    LEFT JOIN offer_status ON offer_status.offer_id = offer.offer_id
    LEFT JOIN offer_tags ON offer_tags.offer_id = offer.offer_id
