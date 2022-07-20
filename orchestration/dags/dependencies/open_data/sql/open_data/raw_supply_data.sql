WITH resa_6mois AS (
    SELECT
        DISTINCT venue_id
    FROM
        `{{ bigquery_analytics_dataset }}`.enriched_booking_data
    WHERE
        booking_status IN ('USED', 'REIMBURSED')
        AND DATE_DIFF(DATE("{{ ds }}"), booking_used_date, MONTH) <= 6
),
lieux_permanents1 AS (
    SELECT
        enriched_offerer_data.offerer_id,
        enriched_offerer_data.offerer_name,
        enriched_offerer_data.offerer_siren AS SIREN,
        siren_data.categorieJuridiqueUniteLegale,
        siren_data_labels.label_categorie_juridique,
        enriched_venue_data.venue_id,
        enriched_venue_data.venue_name,
        enriched_offerer_data.offerer_department_code,
        enriched_venue_data.venue_department_code,
        enriched_venue_data.venue_type_label,
        enriched_offer_data.offer_subcategoryid,
        subcategories.category_id,
        enriched_offer_data.offer_id,
        physical_goods,
        last_stock_price AS stock_price
    FROM
        `{{ bigquery_analytics_dataset }}`.enriched_offerer_data
        JOIN `{{ bigquery_analytics_dataset }}`.enriched_venue_data ON enriched_offerer_data.offerer_id = enriched_venue_data.venue_managing_offerer_id
        AND venue_is_permanent IS TRUE
        AND venue_is_virtual IS FALSE
        JOIN resa_6mois ON enriched_venue_data.venue_id = resa_6mois.venue_id
        JOIN `{{ bigquery_analytics_dataset }}`.enriched_offer_data ON enriched_venue_data.venue_id = enriched_offer_data.venue_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.subcategories ON subcategories.id = enriched_offer_data.offer_subcategoryid
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.siren_data ON enriched_offerer_data.offerer_siren = siren_data.siren
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.siren_data_labels ON siren_data.activitePrincipaleUniteLegale = siren_data_labels.activitePrincipaleUniteLegale
        AND CAST(
            siren_data.categorieJuridiqueUniteLegale AS STRING
        ) = CAST(
            siren_data_labels.categorieJuridiqueUniteLegale AS STRING
        )
),
lieux_permanents AS (
    SELECT
        offerer_id,
        offerer_name,
        SIREN AS siren,
        categorieJuridiqueUniteLegale AS legal_category_unit,
        label_categorie_juridique AS legal_category_label,
        venue_id,
        venue_name,
        offerer_department_code,
        venue_department_code,
        venue_type_label AS venue_type,
        COUNT(DISTINCT venue_id) AS cnt_venues,
        COUNT(DISTINCT offer_id) AS cnt_offers,
        COUNT(
            DISTINCT CASE
                WHEN stock_price > 0 THEN 1
                ELSE NULL
            END
        ) / COUNT(DISTINCT offer_id) AS prct_chargeable_offers,
        COUNT(
            DISTINCT CASE
                WHEN physical_goods THEN 1
                ELSE NULL
            END
        ) / COUNT(DISTINCT offer_id) AS prct_physical_goods,
        COUNT(
            DISTINCT CASE
                WHEN category_id = 'FILM' THEN offer_id
                ELSE NULL
            END
        ) AS cnt_film_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id IN ('CONFERENCE_RENCONTRE', 'BEAUX_ARTS', 'TECHNIQUE') THEN offer_id
                ELSE NULL
            END
        ) AS cnt_other_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id IN ('CINEMA') THEN offer_id
                ELSE NULL
            END
        ) AS cnt_cinema_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id = 'INSTRUMENT' THEN offer_id
                ELSE NULL
            END
        ) AS cnt_instrument_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id = 'JEU' THEN offer_id
                ELSE NULL
            END
        ) AS cnt_games_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id IN ('LIVRE') THEN offer_id
                ELSE NULL
            END
        ) AS cnt_books_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id IN ('MUSEE') THEN offer_id
                ELSE NULL
            END
        ) AS cnt_museums_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id IN ('MUSIQUE_LIVE', 'MUSIQUE_ENREGISTREE') THEN offer_id
                ELSE NULL
            END
        ) AS cnt_music_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id IN ('PRATIQUE_ART') THEN offer_id
                ELSE NULL
            END
        ) AS cnt_artistic_practice_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id IN ('SPECTACLE') THEN offer_id
                ELSE NULL
            END
        ) AS cnt_performing_arts_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id = 'MEDIA' THEN offer_id
                ELSE NULL
            END
        ) AS cnt_media_offers
    FROM
        lieux_permanents1
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10
),
lieux_non_permanents1 AS (
    SELECT
        enriched_offerer_data.offerer_id,
        enriched_offerer_data.offerer_name,
        enriched_offerer_data.offerer_siren AS SIREN,
        siren_data.categorieJuridiqueUniteLegale,
        siren_data_labels.label_categorie_juridique,
        enriched_venue_data.venue_id,
        enriched_venue_data.venue_name,
        enriched_venue_data.venue_is_virtual,
        enriched_offerer_data.offerer_department_code AS offerer_dpt,
        enriched_venue_data.venue_department_code AS venue_dpt,
        enriched_venue_data.venue_type_label AS type_lieu,
        enriched_offer_data.offer_subcategoryid,
        subcategories.category_id,
        enriched_offer_data.offer_id,
        physical_goods,
        last_stock_price AS stock_price
    FROM
        `{{ bigquery_analytics_dataset }}`.enriched_offerer_data
        JOIN `{{ bigquery_analytics_dataset }}`.enriched_venue_data ON enriched_offerer_data.offerer_id = enriched_venue_data.venue_managing_offerer_id
        AND venue_is_permanent IS FALSE
        AND venue_is_virtual IS FALSE
        JOIN resa_6mois ON enriched_venue_data.venue_id = resa_6mois.venue_id
        JOIN `{{ bigquery_analytics_dataset }}`.enriched_offer_data ON enriched_venue_data.venue_id = enriched_offer_data.venue_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.subcategories ON subcategories.id = enriched_offer_data.offer_subcategoryid
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.siren_data ON enriched_offerer_data.offerer_siren = siren_data.siren
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.siren_data_labels ON siren_data.activitePrincipaleUniteLegale = siren_data_labels.activitePrincipaleUniteLegale
        AND CAST(
            siren_data.categorieJuridiqueUniteLegale AS STRING
        ) = CAST(
            siren_data_labels.categorieJuridiqueUniteLegale AS STRING
        )
),
lieux_non_permanents AS (
    SELECT
        offerer_id,
        offerer_name,
        SIREN AS siren,
        categorieJuridiqueUniteLegale AS legal_category_unit,
        label_categorie_juridique AS legal_category_label,
        '_' AS venue_id,
        'Lieux non permanents' AS venue_name,
        offerer_dpt AS offerer_department_code,
        '_' AS venue_department_code,
        '_' AS venue_type,
        COUNT(DISTINCT venue_id) AS cnt_venues,
        COUNT(DISTINCT offer_id) AS cnt_offers,
        COUNT(
            DISTINCT CASE
                WHEN stock_price > 0 THEN 1
                ELSE NULL
            END
        ) / COUNT(DISTINCT offer_id) AS prct_chargeable_offers,
        COUNT(
            DISTINCT CASE
                WHEN physical_goods THEN 1
                ELSE NULL
            END
        ) / COUNT(DISTINCT offer_id) AS prct_physical_goods,
        COUNT(
            DISTINCT CASE
                WHEN category_id = 'FILM' THEN offer_id
                ELSE NULL
            END
        ) AS cnt_film_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id IN ('CONFERENCE_RENCONTRE', 'BEAUX_ARTS', 'TECHNIQUE') THEN offer_id
                ELSE NULL
            END
        ) AS cnt_other_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id IN ('CINEMA') THEN offer_id
                ELSE NULL
            END
        ) AS cnt_cinema_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id = 'INSTRUMENT' THEN offer_id
                ELSE NULL
            END
        ) AS cnt_instrument_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id = 'JEU' THEN offer_id
                ELSE NULL
            END
        ) AS cnt_games_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id IN ('LIVRE') THEN offer_id
                ELSE NULL
            END
        ) AS cnt_books_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id IN ('MUSEE') THEN offer_id
                ELSE NULL
            END
        ) AS cnt_museums_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id IN ('MUSIQUE_LIVE', 'MUSIQUE_ENREGISTREE') THEN offer_id
                ELSE NULL
            END
        ) AS cnt_music_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id IN ('PRATIQUE_ART') THEN offer_id
                ELSE NULL
            END
        ) AS cnt_artistic_practice_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id IN ('SPECTACLE') THEN offer_id
                ELSE NULL
            END
        ) AS cnt_performing_arts_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id = 'MEDIA' THEN offer_id
                ELSE NULL
            END
        ) AS cnt_media_offers
    FROM
        lieux_non_permanents1
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10
),
lieux_virtuels1 AS (
    SELECT
        enriched_offerer_data.offerer_id,
        enriched_offerer_data.offerer_name,
        enriched_offerer_data.offerer_siren AS SIREN,
        siren_data.categorieJuridiqueUniteLegale,
        siren_data_labels.label_categorie_juridique,
        enriched_venue_data.venue_id,
        enriched_venue_data.venue_name,
        enriched_venue_data.venue_is_virtual,
        enriched_offerer_data.offerer_department_code AS offerer_dpt,
        enriched_venue_data.venue_department_code AS venue_dpt,
        enriched_venue_data.venue_type_label AS type_lieu,
        enriched_offer_data.offer_subcategoryid,
        subcategories.category_id,
        enriched_offer_data.offer_id,
        physical_goods,
        last_stock_price AS stock_price
    FROM
        `{{ bigquery_analytics_dataset }}`.enriched_offerer_data
        JOIN `{{ bigquery_analytics_dataset }}`.enriched_venue_data ON enriched_offerer_data.offerer_id = enriched_venue_data.venue_managing_offerer_id
        AND venue_is_virtual IS TRUE
        JOIN resa_6mois ON enriched_venue_data.venue_id = resa_6mois.venue_id
        JOIN `{{ bigquery_analytics_dataset }}`.enriched_offer_data ON enriched_venue_data.venue_id = enriched_offer_data.venue_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.subcategories ON subcategories.id = enriched_offer_data.offer_subcategoryid
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.siren_data ON enriched_offerer_data.offerer_siren = siren_data.siren
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.siren_data_labels ON siren_data.activitePrincipaleUniteLegale = siren_data_labels.activitePrincipaleUniteLegale
        AND CAST(
            siren_data.categorieJuridiqueUniteLegale AS STRING
        ) = CAST(
            siren_data_labels.categorieJuridiqueUniteLegale AS STRING
        )
),
lieux_virtuels AS (
    SELECT
        offerer_id,
        offerer_name,
        SIREN AS siren,
        categorieJuridiqueUniteLegale AS legal_category_unit,
        label_categorie_juridique AS legal_category_label,
        '_' AS venue_id,
        'Offres numériques' AS venue_name,
        offerer_dpt AS offerer_department_code,
        '_' AS venue_department_code,
        '_' AS venue_type,
        COUNT(DISTINCT venue_id) AS cnt_venues,
        COUNT(DISTINCT offer_id) AS cnt_offers,
        COUNT(
            DISTINCT CASE
                WHEN stock_price > 0 THEN 1
                ELSE NULL
            END
        ) / COUNT(DISTINCT offer_id) AS prct_chargeable_offers,
        COUNT(
            DISTINCT CASE
                WHEN physical_goods THEN 1
                ELSE NULL
            END
        ) / COUNT(DISTINCT offer_id) AS prct_physical_goods,
        COUNT(
            DISTINCT CASE
                WHEN category_id = 'FILM' THEN offer_id
                ELSE NULL
            END
        ) AS cnt_film_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id IN ('CONFERENCE_RENCONTRE', 'BEAUX_ARTS', 'TECHNIQUE') THEN offer_id
                ELSE NULL
            END
        ) AS cnt_other_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id IN ('CINEMA') THEN offer_id
                ELSE NULL
            END
        ) AS cnt_cinema_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id = 'INSTRUMENT' THEN offer_id
                ELSE NULL
            END
        ) AS cnt_instrument_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id = 'JEU' THEN offer_id
                ELSE NULL
            END
        ) AS cnt_games_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id IN ('LIVRE') THEN offer_id
                ELSE NULL
            END
        ) AS cnt_books_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id IN ('MUSEE') THEN offer_id
                ELSE NULL
            END
        ) AS cnt_museums_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id IN ('MUSIQUE_LIVE', 'MUSIQUE_ENREGISTREE') THEN offer_id
                ELSE NULL
            END
        ) AS cnt_music_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id IN ('PRATIQUE_ART') THEN offer_id
                ELSE NULL
            END
        ) AS cnt_artistic_practice_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id IN ('SPECTACLE') THEN offer_id
                ELSE NULL
            END
        ) AS cnt_performing_arts_offers,
        COUNT(
            DISTINCT CASE
                WHEN category_id = 'MEDIA' THEN offer_id
                ELSE NULL
            END
        ) AS cnt_media_offers
    FROM
        lieux_virtuels1
    GROUP BY
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10
),

union_lieux AS (
SELECT
    *
FROM
    lieux_permanents
UNION
ALL
SELECT
    *
FROM
    lieux_non_permanents
UNION
ALL
SELECT
    *
FROM
    lieux_virtuels
)


SELECT
    DATE("{{ current_month(ds) }}") as calculation_month,
    *
FROM union_lieux