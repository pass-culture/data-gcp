WITH previous_export AS (
    SELECT
        DISTINCT email
    FROM  `{{ bigquery_clean_dataset }}.qualtrics_ac`
    WHERE calculation_month >= DATE_SUB(DATE("{{ current_month(ds) }}"), INTERVAL 6 MONTH)
),

answers AS (
    SELECT distinct user_id
    FROM `{{ bigquery_analytics_dataset }}.qualtrics_answers`
),

lieux_physique AS (
    SELECT
        global_venue.venue_id,
        venue_booking_email as email,
        venue_type_label,
        DATE_DIFF(current_date, venue_creation_date, DAY) AS anciennete_en_jours,
        total_non_cancelled_bookings,
        total_created_individual_offers,
        total_created_collective_offers,
        total_created_individual_offers + total_created_collective_offers AS offers_created,
        venue_is_permanent,
        global_venue.venue_region_name,
        -- TODO rename field in qualtrics
        global_venue.venue_department_code,
        venue_location.venue_rural_city_type as geo_type,
        venue_location.venue_in_qpv,
        venue_location.venue_in_zrr
    FROM
        `{{ bigquery_analytics_dataset }}.global_venue` global_venue
        LEFT JOIN `{{ bigquery_int_geo_dataset }}.venue_location` venue_location ON venue_location.venue_id = global_venue.venue_id
        LEFT JOIN `{{ bigquery_raw_dataset }}.qualtrics_opt_out_users` opt_out on opt_out.ext_ref = global_venue.venue_id
        LEFT JOIN answers ON global_venue.venue_id = answers.user_id
    WHERE NOT venue_is_virtual
    AND opt_out.contact_id IS NULL
    AND answers.user_id IS NULL
),

generate_export AS (
    SELECT
        lp.*
    FROM lieux_physique lp
    LEFT JOIN previous_export pe on pe.email = lp.email
    WHERE pe.email is null
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
    generate_export
