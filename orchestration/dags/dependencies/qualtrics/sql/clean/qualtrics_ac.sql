WITH lieux_numerique AS (
    SELECT
        enriched_venue_data.venue_id,
        venue_booking_email as email,
        venue_name,
        venue_type_label,
        DATE_DIFF(current_date, venue_creation_date, DAY) AS anciennete_en_jours,
        non_cancelled_bookings,
        individual_offers_created + collective_offers_created AS offers_created,
        theoretic_revenue,
        venue_is_permanent,
        venue_region_name,
        enriched_venue_data.venue_department_code,
        geo_type,
        venue_is_virtual,
        DATE_DIFF(CURRENT_DATE, last_booking_date, DAY) AS nb_jours_depuis_derniere_resa
    FROM
        `{{ bigquery_analytics_dataset }}.enriched_venue_data` enriched_venue_data
        LEFT JOIN `{{ bigquery_analytics_dataset }}.venue_locations` venue_locations ON venue_locations.venue_id = enriched_venue_data.venue_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}.rural_city_type_data` rural_city_type_data ON CAST(rural_city_type_data.geo_code AS string) = CAST(venue_locations.codgeo AS string)
    WHERE
        venue_is_virtual
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
        10,
        11,
        12,
        13,
        14
    ORDER BY
        RAND()
    LIMIT
        {{ params.volume }}
), lieux_physique AS (
    SELECT
        enriched_venue_data.venue_id,
        venue_booking_email as email,
        venue_name,
        venue_type_label,
        DATE_DIFF(current_date, venue_creation_date, DAY) AS anciennete_en_jours,
        non_cancelled_bookings,
        individual_offers_created + collective_offers_created AS offers_created,
        theoretic_revenue,
        venue_is_permanent,
        venue_region_name,
        enriched_venue_data.venue_department_code,
        geo_type,
        venue_is_virtual,
        DATE_DIFF(CURRENT_DATE, last_booking_date, DAY) AS nb_jours_depuis_derniere_resa
    FROM
        `{{ bigquery_analytics_dataset }}.enriched_venue_data` enriched_venue_data
        LEFT JOIN `{{ bigquery_analytics_dataset }}.venue_locations` venue_locations ON venue_locations.venue_id = enriched_venue_data.venue_id
        LEFT JOIN `{{ bigquery_analytics_dataset }}.rural_city_type_data` rural_city_type_data ON CAST(rural_city_type_data.geo_code AS string) = CAST(venue_locations.codgeo AS string)
    WHERE
        NOT venue_is_virtual
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
        10,
        11,
        12,
        13,
        14
    ORDER BY
        RAND()
    LIMIT
        {{ params.volume }}
)


SELECT
    DATE("{{ current_month(ds) }}") as calculation_month,
    CURRENT_DATE as export_date,
    *
FROM
    lieux_numerique
UNION
ALL
SELECT
    DATE("{{ current_month(ds) }}") as calculation_month,
    CURRENT_DATE as export_date,
    *
FROM
    lieux_physique
