{{
    config(
        materialized = "incremental",
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "event_date", "data_type": "date"},
        on_schema_change = "sync_all_columns"
    )
}}

WITH first_clean as(
    SELECT 
        event_name,
        user_pseudo_id,
        CASE WHEN REGEXP_CONTAINS(user_id, r"\D") THEN dehumanize_id(user_id) ELSE user_id END AS user_id,
        platform,
        event_date,
        event_timestamp,
        ga_session_number as session_number,
        ga_session_id as session_id,
        CONCAT(user_pseudo_id, '-',ga_session_id) as unique_session_id,
        from as origin,
        to as destination,
        traffic_campaign,
        traffic_medium,
        traffic_source,
        category as user_device_category,
        operating_system as user_device_operating_system,
        operating_system_version as user_device_operating_system_version,
        browser as user_web_browser,
        browser_version as user_web_browser_version,
        COALESCE(CAST(offerer_id as STRING),CAST(offererId AS STRING)) as offerer_id,
        venue_id,
        page_location,
        page_title as page_name,
        REGEXP_EXTRACT(page_location, r"""passculture\.pro\/(.*)$""", 1) as url_first_path,
        REGEXP_EXTRACT(page_location, r"""passculture\.pro\/(.*?)[\/.*?\/.*?\/|\?]""", 1) as url_path_type,
        REGEXP_EXTRACT(page_location, r"""passculture\.pro\/.*?\/.*?\/(.*?)\/|\?""", 1) as url_path_details,
        REGEXP_EXTRACT(page_location, r"""passculture\.pro\/(.*?)\?""", 1) as url_path_before_params,
        REGEXP_EXTRACT_ALL(page_location,r'(?:\?|&)(?:([^=]+)=(?:[^&]*))') as url_params_key,
        REGEXP_EXTRACT_ALL(page_location,r'(?:\?|&)(?:(?:[^=]+)=([^&]*))') as url_params_value,
        REGEXP_REPLACE(REGEXP_REPLACE(page_location , "[A-Z \\d]+[\\?\\/\\&]?", ""), "https://passculture.pro/", "") as url_path_agg,
        page_referrer,
        page_number,
        COALESCE(double_offer_id,int_offer_id) AS offer_id,
        offerType as offer_type,
        used,
        saved,
        hasOnly6eAnd5eStudents as eac_wrong_student_modal_only6and5,
        isEdition as is_edition,
        isDraft as is_draft,
        filled,
        filledWithErrors as filled_with_errors,
        categorieJuridiqueUniteLegale as onboarding_selected_legal_category
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
FROM first_clean 
