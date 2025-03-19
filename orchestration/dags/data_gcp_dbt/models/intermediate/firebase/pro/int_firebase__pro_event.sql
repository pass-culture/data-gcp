{{ config(pre_hook="{{ create_dehumanize_id_function() }}") }}

{% set target_name = var("ENV_SHORT_NAME") %}
{% set target_schema = generate_schema_name("analytics_" ~ target_name) %}

{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "event_date", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

with
    pro_event_raw_data as (
        select
            event_name,
            user_pseudo_id,
            case
                when regexp_contains(user_id, r"\D")
                then {{ target_schema }}.dehumanize_id(user_id)
                else user_id
            end as user_id,
            platform,
            event_date,
            event_timestamp,
            ga_session_number as session_number,
            ga_session_id as session_id,
            concat(user_pseudo_id, '-', ga_session_id) as unique_session_id,
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
            coalesce(
                cast(offerer_id as string), cast(offererid as string)
            ) as offerer_id,
            coalesce(venue_id, cast(venueid as string)) as venue_id,
            page_location,
            case
                when page_title like "%- Espace partenaires pass Culture%"
                then
                    replace(
                        page_title,
                        "- Espace partenaires pass Culture",
                        "- pass Culture Pro"
                    )
                else page_title
            end as page_name,
            regexp_extract(
                page_location, r"""passculture\.pro\/(.*)$""", 1
            ) as url_first_path,
            regexp_extract(
                page_location, r"""passculture\.pro\/(.*?)[\/.*?\/.*?\/|\?]""", 1
            ) as url_path_type,
            regexp_extract(
                page_location, r"""passculture\.pro\/.*?\/.*?\/(.*?)\/|\?""", 1
            ) as url_path_details,
            regexp_extract(
                page_location, r"""passculture\.pro\/(.*?)\?""", 1
            ) as url_path_before_params,
            regexp_extract_all(
                page_location, r'(?:\?|&)(?:([^=]+)=(?:[^&]*))'
            ) as url_params_key,
            regexp_extract_all(
                page_location, r'(?:\?|&)(?:(?:[^=]+)=([^&]*))'
            ) as url_params_value,
            regexp_replace(
                regexp_replace(page_location, "[A-Z \\d]+[\\?\\/\\&]?", ""),
                "https://passculture.pro/",
                ""
            ) as url_path_agg,
            page_referrer,
            page_number,
            coalesce(
                cast(double_offer_id as string), cast(offerid as string)
            ) as offer_id,
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
            filescount as download_files_cnt,
            subcategoryid as offer_subcategory_id,
            choosensuggestedsubcategory as suggested_offer_subcategory_selected,
            status as offer_status,
            imagecreationstage as image_creation_stage,
            json_extract_array(selected_offers) as selected_offers_array,
            array_length(json_extract_array(selected_offers)) > 1 as multiple_selection,
            actiontype as headline_offer_action_type
        from {{ ref("int_firebase__pro_event_flattened") }}
        {% if is_incremental() %}
            where
                event_date
                between date_sub(date("{{ ds() }}"), interval 2 day) and date(
                    "{{ ds() }}"
                )
        {% endif %}

    ),

    unnested_events as (
        select
            *,
            json_extract_scalar(item, "$.offerId") as offer_id_from_array,
            json_extract_scalar(item, "$.offerStatus") as offer_status_from_array,
            multiple_selection as is_multiple_selection
        from pro_event_raw_data, unnest(selected_offers_array) as item
    )

select
    event_name,
    user_pseudo_id,
    user_id,
    platform,
    event_date,
    event_timestamp,
    session_number,
    session_id,
    unique_session_id,
    origin,
    destination,
    traffic_campaign,
    traffic_medium,
    traffic_source,
    user_device_category,
    user_device_operating_system,
    user_device_operating_system_version,
    user_web_browser,
    user_web_browser_version,
    offerer_id,
    venue_id,
    page_location,
    page_name,
    url_first_path,
    case
        when url_path_details is null
        then replace(coalesce(url_path_before_params, url_first_path), "/", "-")
        when url_path_details is not null
        then concat(url_path_type, "-", url_path_details)
        else url_path_type
    end as url_path_extract,
    url_path_details,
    url_params_key,
    url_params_value,
    url_path_agg,
    download_file_type,
    page_referrer,
    page_number,
    has_saved_query,
    has_opened_wrong_student_modal,
    is_edition,
    is_draft,
    filled,
    filled_with_errors,
    onboarding_selected_legal_category,
    download_format,
    download_booking_status,
    download_button_type,
    download_files_cnt,
    offer_subcategory_id,
    suggested_offer_subcategory_selected,
    is_multiple_selection as multiple_selection,  -- Utilisation du champ correct
    coalesce(cast(offer_id_from_array as string), cast(offer_id as string)) as offer_id,
    coalesce(offer_status_from_array, offer_status) as offer_status,
    cast(offer_type as string) as offer_type,
    image_creation_stage,
    headline_offer_action_type
from unnested_events

union all

select
    event_name,
    user_pseudo_id,
    user_id,
    platform,
    event_date,
    event_timestamp,
    session_number,
    session_id,
    unique_session_id,
    origin,
    destination,
    traffic_campaign,
    traffic_medium,
    traffic_source,
    user_device_category,
    user_device_operating_system,
    user_device_operating_system_version,
    user_web_browser,
    user_web_browser_version,
    offerer_id,
    venue_id,
    page_location,
    page_name,
    url_first_path,
    case
        when url_path_details is null
        then replace(coalesce(url_path_before_params, url_first_path), "/", "-")
        when url_path_details is not null
        then concat(url_path_type, "-", url_path_details)
        else url_path_type
    end as url_path_extract,
    url_path_details,
    url_params_key,
    url_params_value,
    url_path_agg,
    download_file_type,
    page_referrer,
    page_number,
    has_saved_query,
    has_opened_wrong_student_modal,
    is_edition,
    is_draft,
    filled,
    filled_with_errors,
    onboarding_selected_legal_category,
    download_format,
    download_booking_status,
    download_button_type,
    download_files_cnt,
    offer_subcategory_id,
    suggested_offer_subcategory_selected,
    false as multiple_selection,
    cast(offer_id as string) as offer_id,
    cast(offer_status as string) as offer_status,
    cast(offer_type as string) as offer_type,
    image_creation_stage,
    headline_offer_action_type
from pro_event_raw_data
where selected_offers_array is null
