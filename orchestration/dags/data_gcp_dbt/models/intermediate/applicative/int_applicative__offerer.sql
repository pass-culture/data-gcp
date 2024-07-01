{% set target_name = target.name %}
{% set target_schema = generate_schema_name('analytics_' ~ target_name) %}

{{ config(
    pre_hook="{{create_humanize_id_function()}}"
) }}

WITH venue_grouped_by_offerer AS (
    SELECT venue_managing_offerer_id,
        SUM(total_non_cancelled_individual_bookings) AS total_non_cancelled_individual_bookings,
        SUM(total_used_individual_bookings) AS total_used_individual_bookings,
        SUM(total_individual_theoretic_revenue) AS total_individual_theoretic_revenue,
        SUM(total_individual_real_revenue) AS total_individual_real_revenue,
        SUM(total_individual_current_year_real_revenue) AS total_individual_current_year_real_revenue,
        MIN(first_individual_booking_date) AS first_individual_booking_date,
        MAX(last_individual_booking_date) AS last_individual_booking_date,
        MIN(first_individual_offer_creation_date) AS first_individual_offer_creation_date,
        MAX(last_individual_offer_creation_date) AS last_individual_offer_creation_date,
        SUM(total_created_individual_offers) AS total_created_individual_offers,
        SUM(total_bookable_individual_offers) AS total_bookable_individual_offers,
        MIN(first_stock_creation_date) AS first_stock_creation_date,
        SUM(total_non_cancelled_collective_bookings) AS total_non_cancelled_collective_bookings,
        SUM(total_used_collective_bookings) AS total_used_collective_bookings,
        SUM(total_collective_theoretic_revenue) AS total_collective_theoretic_revenue,
        SUM(total_collective_real_revenue) AS total_collective_real_revenue,
        SUM(total_collective_current_year_real_revenue) AS total_collective_current_year_real_revenue,
        MIN(first_collective_booking_date) AS first_collective_booking_date,
        MAX(last_collective_booking_date) AS last_collective_booking_date,
        MIN(first_collective_offer_creation_date) AS first_collective_offer_creation_date,
        MAX(last_collective_offer_creation_date) AS last_collective_offer_creation_date,
        SUM(total_created_collective_offers) AS total_created_collective_offers,
        SUM(total_bookable_collective_offers) AS total_bookable_collective_offers,
        SUM(total_venues) AS total_venues,
        SUM(total_non_cancelled_bookings) AS total_non_cancelled_bookings,
        SUM(total_used_bookings) AS total_used_bookings,
        SUM(total_theoretic_revenue) AS total_theoretic_revenue,
        SUM(total_real_revenue) AS total_real_revenue,
        SUM(total_created_offers) AS total_created_offers,
        SUM(total_bookable_offers) AS total_bookable_offers,
        MIN(first_bookable_offer_date) AS first_bookable_offer_date,
        MAX(last_bookable_offer_date) AS last_bookable_offer_date,
        MIN(first_individual_bookable_offer_date) AS first_individual_bookable_offer_date,
        MAX(last_individual_bookable_offer_date) AS last_individual_bookable_offer_date,
        MIN(first_collective_bookable_offer_date) AS first_collective_bookable_offer_date,
        MAX(last_collective_bookable_offer_date) AS last_collective_bookable_offer_date,
        COUNT(DISTINCT venue_id) AS total_managed_venues,
        COUNT(DISTINCT CASE WHEN NOT venue_is_virtual THEN venue_id ELSE NULL END) AS total_physical_managed_venues,
        COUNT(DISTINCT CASE WHEN venue_is_permanent THEN venue_id ELSE NULL END) AS total_permanent_managed_venues
    FROM {{ ref('int_applicative__venue') }}
    GROUP BY venue_managing_offerer_id
)

SELECT o.offerer_is_active,
    o.offerer_address,
    o.offerer_postal_code,
    o.offerer_city,
    o.offerer_id,
    CONCAT("offerer-", o.offerer_id) AS partner_id,
    o.offerer_creation_date,
    o.offerer_name,
    o.offerer_siren,
    o.offerer_validation_status,
    o.offerer_validation_date,
    {{target_schema}}.humanize_id(o.offerer_id) AS offerer_humanized_id,
    CASE
        WHEN o.offerer_postal_code = '97150' THEN '978'
        WHEN SUBSTRING(o.offerer_postal_code, 0, 2) = '97' THEN SUBSTRING(o.offerer_postal_code, 0, 3)
        WHEN SUBSTRING(o.offerer_postal_code, 0, 2) = '98' THEN SUBSTRING(o.offerer_postal_code, 0, 3)
        WHEN SUBSTRING(o.offerer_postal_code, 0, 3) in ('200', '201', '209', '205') THEN '2A'
        WHEN SUBSTRING(o.offerer_postal_code, 0, 3) in ('202', '206') THEN '2B'
    ELSE SUBSTRING(offerer_postal_code, 0, 2)
    END AS offerer_department_code,
    vgo.total_non_cancelled_individual_bookings,
    vgo.total_used_individual_bookings,
    vgo.total_individual_theoretic_revenue,
    vgo.total_individual_real_revenue,
    vgo.first_individual_booking_date,
    vgo.last_individual_booking_date,
    vgo.first_individual_offer_creation_date,
    vgo.last_individual_offer_creation_date,
    vgo.total_bookable_individual_offers,
    vgo.total_bookable_collective_offers,
    vgo.total_created_individual_offers,
    vgo.total_created_collective_offers,
    vgo.first_stock_creation_date,
    vgo.total_non_cancelled_collective_bookings,
    vgo.total_used_collective_bookings,
    vgo.total_collective_theoretic_revenue,
    vgo.total_collective_real_revenue,
    vgo.total_individual_current_year_real_revenue + vgo.total_collective_current_year_real_revenue AS total_current_year_real_revenue,
    vgo.first_collective_booking_date,
    vgo.last_collective_booking_date,
    vgo.first_collective_offer_creation_date,
    vgo.last_collective_offer_creation_date,
    vgo.total_non_cancelled_bookings,
    vgo.total_used_bookings,
    vgo.total_theoretic_revenue,
    vgo.total_real_revenue,
    vgo.total_created_offers,
    vgo.total_bookable_offers,
    vgo.first_bookable_offer_date,
    vgo.last_bookable_offer_date,
    vgo.first_individual_bookable_offer_date,
    vgo.last_individual_bookable_offer_date,
    vgo.first_collective_bookable_offer_date,
    vgo.last_collective_bookable_offer_date,
    vgo.total_venues,
    vgo.total_managed_venues,
    vgo.total_physical_managed_venues,
    vgo.total_permanent_managed_venues,
    CASE WHEN vgo.first_individual_offer_creation_date IS NOT NULL AND vgo.first_collective_offer_creation_date IS NOT NULL THEN LEAST(vgo.first_collective_offer_creation_date,vgo.first_individual_offer_creation_date)
         ELSE COALESCE(vgo.first_individual_offer_creation_date,vgo.first_collective_offer_creation_date) END AS first_offer_creation_date,
    CASE WHEN vgo.last_individual_offer_creation_date IS NOT NULL AND vgo.last_collective_offer_creation_date IS NOT NULL THEN LEAST(vgo.last_collective_offer_creation_date,vgo.last_individual_offer_creation_date)
         ELSE COALESCE(vgo.last_individual_offer_creation_date,vgo.last_collective_offer_creation_date) END AS last_offer_creation_date,
    CASE WHEN vgo.first_individual_booking_date IS NOT NULL AND vgo.first_collective_booking_date IS NOT NULL THEN LEAST(vgo.first_collective_booking_date,vgo.first_individual_booking_date)
         ELSE COALESCE(vgo.first_individual_booking_date,vgo.first_collective_booking_date) END AS first_booking_date,
    CASE WHEN vgo.last_individual_booking_date IS NOT NULL AND vgo.last_collective_booking_date IS NOT NULL THEN LEAST(vgo.last_collective_booking_date,vgo.last_individual_booking_date)
         ELSE COALESCE(vgo.last_individual_booking_date,vgo.last_collective_booking_date) END AS last_booking_date,
    CASE WHEN DATE_DIFF(CURRENT_DATE,vgo.last_bookable_offer_date,DAY) <= 30 THEN TRUE ELSE FALSE END AS is_active_last_30days,
    CASE WHEN DATE_DIFF(CURRENT_DATE,vgo.last_bookable_offer_date,YEAR) = 0 THEN TRUE ELSE FALSE END AS is_active_current_year,
    CASE WHEN DATE_DIFF(CURRENT_DATE,vgo.last_individual_bookable_offer_date,DAY) <= 30 THEN TRUE ELSE FALSE END AS is_individual_active_last_30days,
    CASE WHEN DATE_DIFF(CURRENT_DATE,vgo.last_individual_bookable_offer_date,YEAR) = 0 THEN TRUE ELSE FALSE END AS is_individual_active_current_year,
    CASE WHEN DATE_DIFF(CURRENT_DATE,vgo.last_collective_bookable_offer_date,DAY) <= 30 THEN TRUE ELSE FALSE END AS is_collective_active_last_30days,
    CASE WHEN DATE_DIFF(CURRENT_DATE,vgo.last_collective_bookable_offer_date,YEAR) = 0 THEN TRUE ELSE FALSE END AS is_collective_active_current_year,
FROM {{ source("raw", "applicative_database_offerer") }} AS o
LEFT JOIN venue_grouped_by_offerer AS vgo ON o.offerer_id = vgo.venue_managing_offerer_id
