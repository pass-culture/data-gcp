{{ config(pre_hook="{{ create_dehumanize_id_function() }}") }}

{% set target_name = target.name %}
{% set target_schema = generate_schema_name('analytics_' ~ target_name) %}

{{
    config(
        **custom_incremental_config(
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "event_date", "data_type": "date"},
        on_schema_change = "sync_all_columns"
    )
) }}

with pro_event_raw_data as (
    select
        event_name,
        user_pseudo_id,
        case when REGEXP_CONTAINS(user_id, r"\D") then {{ target_schema }}.dehumanize_id(user_id) else user_id end as user_id,
        platform,
        event_date,
        event_timestamp,
        ga_session_number as session_number,
        ga_session_id as session_id,
        CONCAT(user_pseudo_id, '-', ga_session_id) as unique_session_id,
        origin,
        destination,
        traffic_campaign,
        traffic_medium,
        traffic_source,
        category as user_device_category,
        operating_system as user_device_operating_system,
        operating_system_version as user_device_operating_system_version,
        browser as user_web_browser,
        browser_version as user_web_browser_version,
        COALESCE(CAST(offerer_id as STRING), CAST(offererid as STRING)) as offerer_id,
        COALESCE(venue_id, CAST(venueid as STRING)) as venue_id,
        page_location,
        page_title as page_name,
        REGEXP_EXTRACT(page_location, r"""passculture\.pro\/(.*)$""", 1) as url_first_path,
        REGEXP_EXTRACT(page_location, r"""passculture\.pro\/(.*?)[\/.*?\/.*?\/|\?]""", 1) as url_path_type,
        REGEXP_EXTRACT(page_location, r"""passculture\.pro\/.*?\/.*?\/(.*?)\/|\?""", 1) as url_path_details,
        REGEXP_EXTRACT(page_location, r"""passculture\.pro\/(.*?)\?""", 1) as url_path_before_params,
        REGEXP_EXTRACT_ALL(page_location, r'(?:\?|&)(?:([^=]+)=(?:[^&]*))') as url_params_key,
        REGEXP_EXTRACT_ALL(page_location, r'(?:\?|&)(?:(?:[^=]+)=([^&]*))') as url_params_value,
        REGEXP_REPLACE(REGEXP_REPLACE(page_location, "[A-Z \\d]+[\\?\\/\\&]?", ""), "https://passculture.pro/", "") as url_path_agg,
        page_referrer,
        page_number,
        COALESCE(double_offer_id, offerid) as offer_id,
        offertype as offer_type,
        saved as has_saved_query,
        hasonly6eand5estudents as has_opened_wrong_student_modal,
        isedition as is_edition,
        isdraft as is_draft,
        filled,
        filledwitherrors as filled_with_errors,
        categoriejuridiqueunitelegale as onboarding_selected_legal_category,
        format as download_format,
        bookingstatus as download_booking_status,
        buttontype as download_button_type,
        filetype as download_file_type,
        filescount as download_files_cnt
    from {{ ref("int_firebase__pro_event_flattened") }}
    {% if is_incremental() %}
        where event_date between DATE_SUB(DATE("{{ ds() }}"), interval 2 day) and DATE("{{ ds() }}")
    {% endif %}
)

select
    * except (url_first_path, url_path_type, url_path_details, url_path_before_params),
    case
        when url_path_details is NULL then REPLACE(COALESCE(url_path_before_params, url_first_path), "/", "-")
        when url_path_details is not NULL then CONCAT(url_path_type, "-", url_path_details)
        else url_path_type
    end as url_path_extract
from pro_event_raw_data
