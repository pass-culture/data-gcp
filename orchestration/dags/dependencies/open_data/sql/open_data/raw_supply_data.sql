WITH resa_6mois AS (
    SELECT
        DISTINCT venue_id
    FROM
        `{{ bigquery_analytics_dataset }}`.enriched_booking_data
    WHERE
        booking_status IN ('USED', 'REIMBURSED')
        AND DATE_DIFF(DATE("{{ ds }}"), booking_used_date, MONTH) <= 6
    UNION ALL
    SELECT
      DISTINCT venue_id
    FROM
        `{{ bigquery_analytics_dataset }}`.enriched_collective_booking_data
    WHERE
        collective_booking_status IN ('USED', 'REIMBURSED')
        AND DATE_DIFF(DATE("{{ ds }}"), collective_booking_used_date, MONTH) <= 6
),
all_offers AS (
  SELECT
    venue_id,
    offer_id,
    offer_subcategoryid,
    category_id,
    subcategories.is_physical_deposit AS physical_goods,
    last_stock_price
  FROM `{{ bigquery_analytics_dataset }}`.enriched_offer_data
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.subcategories ON subcategories.id = enriched_offer_data.offer_subcategoryid
  UNION ALL
  SELECT
    venue_id,
    collective_offer_id,
    collective_offer_subcategory_id,
    collective_offer_category_id,
    subcategories.is_physical_deposit AS physical_goods,
    collective_stock_price
  FROM `{{ bigquery_analytics_dataset }}`.enriched_collective_offer_data
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.subcategories ON subcategories.id = enriched_collective_offer_data.collective_offer_subcategory_id
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
        all_offers.offer_subcategoryid,
        all_offers.category_id,
        all_offers.offer_id,
        physical_goods,
        last_stock_price AS stock_price
    FROM
        `{{ bigquery_analytics_dataset }}`.enriched_offerer_data
        JOIN `{{ bigquery_analytics_dataset }}`.enriched_venue_data ON enriched_offerer_data.offerer_id = enriched_venue_data.venue_managing_offerer_id
        AND venue_is_permanent IS TRUE
        AND venue_is_virtual IS FALSE
        JOIN resa_6mois ON enriched_venue_data.venue_id = resa_6mois.venue_id
        JOIN all_offers ON all_offers.venue_id = resa_6mois.venue_id
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
        ) AS cnt_chargeable_offers,
        COUNT(
            DISTINCT CASE
                WHEN physical_goods THEN 1
                ELSE NULL
            END
        ) AS cnt_physical_goods,
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
        all_offers.offer_subcategoryid,
        all_offers.category_id,
        all_offers.offer_id,
        all_offers.physical_goods,
        all_offers.last_stock_price AS stock_price
    FROM
        `{{ bigquery_analytics_dataset }}`.enriched_offerer_data
        JOIN `{{ bigquery_analytics_dataset }}`.enriched_venue_data ON enriched_offerer_data.offerer_id = enriched_venue_data.venue_managing_offerer_id
        AND venue_is_permanent IS FALSE
        AND venue_is_virtual IS FALSE
        JOIN resa_6mois ON enriched_venue_data.venue_id = resa_6mois.venue_id
        JOIN all_offers ON all_offers.venue_id = resa_6mois.venue_id
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
        ) AS cnt_chargeable_offers,
        COUNT(
            DISTINCT CASE
                WHEN physical_goods THEN 1
                ELSE NULL
            END
        ) AS cnt_physical_goods,
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
        all_offers.offer_subcategoryid,
        all_offers.category_id,
        all_offers.offer_id,
        all_offers.physical_goods,
        all_offers.last_stock_price AS stock_price
    FROM
        `{{ bigquery_analytics_dataset }}`.enriched_offerer_data
        JOIN `{{ bigquery_analytics_dataset }}`.enriched_venue_data ON enriched_offerer_data.offerer_id = enriched_venue_data.venue_managing_offerer_id
        AND venue_is_virtual IS TRUE
        JOIN resa_6mois ON enriched_venue_data.venue_id = resa_6mois.venue_id
        JOIN all_offers ON all_offers.venue_id = resa_6mois.venue_id
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
        ) cnt_chargeable_offers,
        COUNT(
            DISTINCT CASE
                WHEN physical_goods THEN 1
                ELSE NULL
            END
        ) cnt_physical_goods,
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
WHERE venue_type in (
    "Spectacle vivant",
    "Patrimoine et tourisme",
    "Offre numérique",
    "Musique - Salle de concerts",
    "Musique - Magasin d’instruments",
    "Musique - Disquaire",
    "Musée",
    "Magasin arts créatifs",
    "Lieu administratif",
    "Librairie",
    "Jeux / Jeux vidéos",
    "Festival",
    "Culture scientifique",
    "Cours et pratique artistiques",
    "Cinéma - Salle de projections",
    "Centre culturel",
    "Bibliothèque ou médiathèque",
    "Autre",
    "Arts visuels, arts plastiques et galeries",
    "_"
)