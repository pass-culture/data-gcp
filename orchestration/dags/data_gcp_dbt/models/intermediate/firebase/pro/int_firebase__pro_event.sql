{{ config(
    pre_hook="{{ create_humanize_id_function() }}"
) }}

{% set target_name = target.name %}
{% set target_schema = generate_schema_name('analytics_dbt_' ~ target_name) %}

{{
    config(
        materialized = "incremental",
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "event_date", "data_type": "date"},
        on_schema_change = "sync_all_columns"
    )
}}

WITH pro_event_raw_data as(
    SELECT 
        event_name,
        user_pseudo_id,
        CASE WHEN REGEXP_CONTAINS(user_id, r"\D") THEN {{target_schema}}.dehumanize_id(user_id) ELSE user_id END AS user_id,
        platform,
        event_date,
        event_timestamp,
        ga_session_number AS session_number,
        ga_session_id AS session_id,
        CONCAT(user_pseudo_id, '-',ga_session_id) AS unique_session_id,
        origin AS page_origin,
        destination,
        traffic_campaign,
        traffic_medium,
        traffic_source,
        category AS user_device_category,
        operating_system AS user_device_operating_system,
        operating_system_version AS user_device_operating_system_version,
        browser AS user_web_browser,
        browser_version AS user_web_browser_version,
        COALESCE(CAST(offerer_id AS STRING),CAST(offererId AS STRING)) AS offerer_id,
        COALESCE(venue_id,CAST(venueId AS STRING)) AS venue_id,
        page_location,
        page_title AS page_name,
        REGEXP_EXTRACT(page_location, r"""passculture\.pro\/(.*)$""", 1) AS url_first_path,
        REGEXP_EXTRACT(page_location, r"""passculture\.pro\/(.*?)[\/.*?\/.*?\/|\?]""", 1) AS url_path_type,
        REGEXP_EXTRACT(page_location, r"""passculture\.pro\/.*?\/.*?\/(.*?)\/|\?""", 1) AS url_path_details,
        REGEXP_EXTRACT(page_location, r"""passculture\.pro\/(.*?)\?""", 1) AS url_path_before_params,
        REGEXP_EXTRACT_ALL(page_location,r'(?:\?|&)(?:([^=]+)=(?:[^&]*))') AS url_params_key,
        REGEXP_EXTRACT_ALL(page_location,r'(?:\?|&)(?:(?:[^=]+)=([^&]*))') AS url_params_value,
        REGEXP_REPLACE(REGEXP_REPLACE(page_location , "[A-Z \\d]+[\\?\\/\\&]?", ""), "https://passculture.pro/", "") AS url_path_agg,
        page_referrer,
        page_number,
        COALESCE(double_offer_id,offerId) AS offer_id,
        offerType AS offer_type,
        saved AS has_saved_query,
        hasOnly6eAnd5eStudents AS has_opened_wrong_student_modal,
        isEdition AS is_edition,
        isDraft AS is_draft,
        filled,
        filledWithErrors AS filled_with_errors,
        categorieJuridiqueUniteLegale AS onboarding_selected_legal_category
FROM {{ ref("int_firebase__pro_event_flattened") }}
{% if is_incremental() %}
WHERE event_date BETWEEN date_sub(DATE("{{ ds() }}"), INTERVAL 2 DAY) and DATE("{{ ds() }}")
{% endif %}
)

SELECT
    * EXCEPT(url_first_path, url_path_type, url_path_details, url_path_before_params),
    CASE 
        WHEN url_path_details is NULL THEN REPLACE(COALESCE(url_path_before_params, url_first_path), "/", "-")
        WHEN url_path_details is NOT NULL THEN CONCAT(url_path_type, "-", url_path_details)
        ELSE url_path_type 
    END as url_path_extract
FROM pro_event_raw_data
